// Copyright 2020 The Chubao Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
// implied. See the License for the specific language governing
// permissions and limitations under the License.

package ecnode

import (
	"encoding/json"
	"fmt"
	"github.com/chubaofs/chubaofs/proto"
	"github.com/chubaofs/chubaofs/repl"
	"github.com/chubaofs/chubaofs/storage"
	"github.com/chubaofs/chubaofs/util/errors"
	"github.com/chubaofs/chubaofs/util/log"
	"hash/crc32"
	"io/ioutil"
	"math"
	"net"
	"os"
	"path"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

const (
	EcPartitionPrefix           = "ecpartition"
	EcPartitionMetaDataFileName = "META"
	TempMetaDataFileName        = ".meta"
	TimeLayout                  = "2006-01-02 15:04:05"

	IntervalToUpdatePartitionSize = 60 // interval to update the partition size
)

type EcPartition struct {
	EcPartitionMetaData

	partitionStatus int
	disk            *Disk
	path            string
	used            uint64
	extentStore     *storage.ExtentStore
	storeC          chan uint64
	stopC           chan bool

	intervalToUpdatePartitionSize int64
	loadExtentHeaderStatus        int
	metaFileLock                  sync.Mutex
}

type EcPartitionMetaData struct {
	PartitionID    uint64   `json:"partition_id"`
	PartitionSize  uint64   `json:"partition_size"`
	VolumeID       string   `json:"vol_name"`
	StripeUnitSize uint64   `json:"stripe_unit_size"`
	ExtentFileSize uint64   `json:"extent_file_size"`
	DataNodeNum    uint32   `json:"data_node_num"`
	ParityNodeNum  uint32   `json:"parity_node_num"`
	NodeIndex      uint32   `json:"node_index"`
	Hosts          []string `json:"hosts"`

	CreateTime string `json:"create_time"`
}

// Disk returns the disk instance.
func (ep *EcPartition) Disk() *Disk {
	return ep.disk
}

func (ep *EcPartition) IsRejectWrite() bool {
	return ep.Disk().RejectWrite
}

// Status returns the partition status.
func (ep *EcPartition) Status() int {
	return ep.partitionStatus
}

// Size returns the partition size.
func (ep *EcPartition) Size() uint64 {
	return ep.PartitionSize
}

// Used returns the used space.
func (ep *EcPartition) Used() uint64 {
	return ep.used
}

// Available returns the available space.
func (ep *EcPartition) Available() uint64 {
	return ep.PartitionSize - ep.used
}

func (ep *EcPartition) GetExtentCount() int {
	return ep.extentStore.GetExtentCount()
}

func (ep *EcPartition) Path() string {
	return ep.path
}

func (ep *EcPartition) ExtentStore() *storage.ExtentStore {
	return ep.extentStore
}

func (ep *EcPartition) checkIsDiskError(err error) (diskError bool) {
	if err == nil {
		return
	}

	if IsDiskErr(err.Error()) {
		mesg := fmt.Sprintf("disk path %v error on %v", ep.Path(), localIP)
		log.LogErrorf(mesg)
		ep.disk.incReadErrCnt()
		ep.disk.incWriteErrCnt()
		ep.disk.Status = proto.Unavailable
		ep.statusUpdate()
		diskError = true
	}
	return
}

func (ep *EcPartition) computeUsage() {
	var (
		used  int64
		files []os.FileInfo
		err   error
	)
	if time.Now().Unix()-ep.intervalToUpdatePartitionSize < IntervalToUpdatePartitionSize {
		return
	}
	if files, err = ioutil.ReadDir(ep.path); err != nil {
		return
	}
	for _, file := range files {
		isExtent := storage.RegexpExtentFile.MatchString(file.Name())
		if !isExtent {
			continue
		}
		used += file.Size()
	}
	ep.used = uint64(used)
	ep.intervalToUpdatePartitionSize = time.Now().Unix()
}

func (ep *EcPartition) statusUpdate() {
	status := proto.ReadWrite
	ep.computeUsage()

	if ep.used >= ep.PartitionSize {
		status = proto.ReadOnly
	}
	if ep.extentStore.GetExtentCount() >= storage.MaxExtentCount {
		status = proto.ReadOnly
	}
	if ep.Status() == proto.Unavailable {
		status = proto.Unavailable
	}

	ep.partitionStatus = int(math.Min(float64(status), float64(ep.disk.Status)))
}

func (ep *EcPartition) statusUpdateScheduler() {
	ticker := time.NewTicker(time.Minute)
	for {
		select {
		case <-ticker.C:
			ep.statusUpdate()
		case <-ep.stopC:
			ticker.Stop()
			return
		}
	}
}

// PersistMetaData persists the file metadata on the disk
func (ep EcPartition) PersistMetaData() (err error) {
	ep.metaFileLock.Lock()
	defer ep.metaFileLock.Unlock()

	tempFileFilePath := path.Join(ep.Path(), TempMetaDataFileName)
	metadataFile, err := os.OpenFile(tempFileFilePath, os.O_CREATE|os.O_RDWR, 0666)
	if err != nil {
		return
	}

	defer func() {
		_ = os.Remove(tempFileFilePath)
	}()

	metadata, err := json.Marshal(ep.EcPartitionMetaData)
	if err != nil {
		return
	}

	_, err = metadataFile.Write(metadata)
	if err != nil {
		return
	}

	err = metadataFile.Sync()
	if err != nil {
		return
	}

	err = metadataFile.Close()
	if err != nil {
		return
	}

	log.LogInfof("PersistMetaData EcPartition(%v) data(%v)", ep.PartitionID, string(metadata))
	err = os.Rename(tempFileFilePath, path.Join(ep.Path(), EcPartitionMetaDataFileName))

	return
}

// newEcPartition
func newEcPartition(epMetaData *EcPartitionMetaData, disk *Disk) (ep *EcPartition, err error) {
	partitionID := epMetaData.PartitionID
	dataPath := path.Join(disk.Path, fmt.Sprintf(EcPartitionPrefix+"_%v_%v", partitionID, epMetaData.PartitionSize))
	partition := &EcPartition{
		EcPartitionMetaData: *epMetaData,
		disk:                disk,
		path:                dataPath,
		stopC:               make(chan bool, 0),
		storeC:              make(chan uint64, 128),
		partitionStatus:     proto.ReadWrite,
	}

	partition.extentStore, err = storage.NewExtentStore(partition.path, partition.PartitionID, int(partition.PartitionSize))
	if err != nil {
		return
	}

	disk.AttachEcPartition(partition)
	ep = partition
	go partition.statusUpdateScheduler()
	return
}

// LoadEcPartition load partition from the specified directory when ecnode start
func LoadEcPartition(partitionDir string, disk *Disk) (ep *EcPartition, err error) {
	metaDataRaw, err := ioutil.ReadFile(path.Join(partitionDir, EcPartitionMetaDataFileName))
	if err != nil {
		return
	}

	metaData := &EcPartitionMetaData{}
	err = json.Unmarshal(metaDataRaw, metaData)
	if err != nil {
		return
	}

	volumeID := strings.TrimSpace(metaData.VolumeID)
	if len(volumeID) == 0 || metaData.PartitionID == 0 || metaData.PartitionSize == 0 {
		return
	}

	ep, err = newEcPartition(metaData, disk)
	if err != nil {
		return
	}

	disk.space.AttachPartition(ep)
	disk.AddSize(uint64(ep.Size()))
	return
}

// CreateEcPartition create ec partition and return its instance
func CreateEcPartition(epMetaData *EcPartitionMetaData, disk *Disk) (ep *EcPartition, err error) {
	ep, err = newEcPartition(epMetaData, disk)
	if err != nil {
		return
	}

	err = ep.PersistMetaData()
	if err != nil {
		return
	}

	disk.AddSize(uint64(ep.Size()))
	return
}

func (ep *EcPartition) localRead(extentID uint64, offset int64, size uint32) ([]byte, error) {
	store := ep.ExtentStore()

	data := make([]byte, size)

	_, err := store.Read(extentID, offset, int64(size), data, false)
	ep.checkIsDiskError(err)
	return data, err
}

func (ep *EcPartition) remoteRead(nodeIndex uint32, extentID uint64, offset int64, size uint32) (data []byte, err error) {
	request := proto.NewPacket()
	request.ReqID = proto.GenerateRequestID()
	request.Opcode = proto.OpStreamRead
	request.Size = size
	request.ExtentOffset = offset
	request.PartitionID = ep.PartitionID
	request.ExtentID = extentID
	request.Magic = proto.ProtoMagic

	conn, err := net.Dial("tcp", ep.Hosts[nodeIndex])
	if err != nil {
		return
	}
	defer conn.Close()

	err = request.WriteToConn(conn)
	if err != nil {
		err = errors.New(fmt.Sprintf("ExtentStripeRead to host(%v) error(%v)", ep.Hosts[nodeIndex], err))
		log.LogWarnf("action[streamRepairExtent] err(%v).", err)
		return
	}

	err = request.ReadFromConn(conn, proto.ReadDeadlineTime) // read the response
	if err != nil {
		err = errors.New(fmt.Sprintf("Stripe RemoteRead EcPartition(%v) from host(%v) error(%v)", ep.PartitionID,
			ep.Hosts[nodeIndex], err))
		return
	}
	if request.ResultCode != proto.OpOk {
		err = errors.New(fmt.Sprintf("Stripe RemoteRead EcPartition(%v) from host(%v) error(%v) resultCode(%v)",
			ep.PartitionID, ep.Hosts[nodeIndex], err, request.ResultCode))
		return
	}

	data = request.Data

	return
}

func (ep *EcPartition) GetNewExtentId() (uint64, error) {
	return ep.extentStore.NextExtentID()
}

func (ep *EcPartition) GetExtentInfo(extentId uint64) (*storage.ExtentInfo, error) {
	return ep.extentStore.Watermark(extentId)
}


func (ep *EcPartition) updatePartitionLocal(data []byte) (err error) {
	request, err := getChangeEcPartitionMemberRequest(data)
	if err != nil {
		return err
	}

	newHosts := request.Hosts
	ep.Hosts = newHosts
	err = ep.PersistMetaData()
	return
}

func getChangeEcPartitionMemberRequest(data []byte) (*proto.ChangeEcPartitionMembersRequest, error) {
	request := &proto.ChangeEcPartitionMembersRequest{}
	task := proto.AdminTask{}
	task.Request = request
	err := json.Unmarshal(data, &task)
	if err != nil {
		return nil, err
	}

	return request, nil
}

func (ep *EcPartition) changeMember(e *EcNode, r *proto.ChangeEcPartitionMembersRequest, data []byte) error {
	err := ep.updatePartitionAllEcNode(e, r, data)
	if err != nil {
		return err
	}

	m, list, err := ep.listExtentsAllEcNode(e, r)
	if err != nil {
		return err
	}

	if isChangeMember(m, list) {
		e.repairEvent <- repairEvent{ep: *ep, maxExtentInfoEcNode: m, extentInfoEcNodeList: list}
	} else {
		log.LogWarnf("PartitionID[%v] no need change member. maxExtentInfoEcNode:(nodeAddr:%v maxExtentId:%v extent len:%v) list len(%v) ",
			ep.PartitionID, m.nodeAddr, m.maxExtentId, len(m.extentInfoMap), len(list))
	}
	return nil
}

func isChangeMember(m *extentInfoEcNode, list []*extentInfoEcNode) bool {
	return m != nil && len(m.extentInfoMap) > 0 && len(list) > 0
}

func (ep *EcPartition) listExtentsAllEcNode(e *EcNode, r *proto.ChangeEcPartitionMembersRequest) (*extentInfoEcNode, []*extentInfoEcNode, error) {
	extentInfoEcNodeList := make([]*extentInfoEcNode, len(r.Hosts))
	wg := sync.WaitGroup{}
	wg.Add(len(r.Hosts))
	for i, host := range r.Hosts {
		go ep.listExtents(i, host, e, &wg, extentInfoEcNodeList)
	}
	wg.Wait()

	if len(extentInfoEcNodeList) < int(ep.DataNodeNum) {
		return nil, nil, errors.NewErrorf("length of extentInfoEcNodeList cannot less than DataNodeNum, PartitionID(%v) length(%v) DataNodeNum(%v)",
			ep.PartitionID, len(extentInfoEcNodeList), ep.DataNodeNum)
	}

	var maxExtentIndex = 0
	var maxExtentId = extentInfoEcNodeList[0].maxExtentId
	for i, extent := range extentInfoEcNodeList {
		if extent.maxExtentId > maxExtentId {
			maxExtentId = extent.maxExtentId
			maxExtentIndex = i
		}
	}

	maxExtentInfoEcNode := extentInfoEcNodeList[maxExtentIndex]
	return maxExtentInfoEcNode, extentInfoEcNodeList, nil
}

func (ep *EcPartition) updatePartition(nodeAddr string, data []byte, e *EcNode, wg *sync.WaitGroup, count *int32) {
	defer wg.Done()
	if nodeAddr == e.localServerAddr {
		// change member local
		err := ep.updatePartitionLocal(data)
		if err != nil {
			log.LogErrorf("updatePartitionLocal fail, PartitionID(%v), err:%v", ep.PartitionID, err)
			return
		}

		atomic.AddInt32(count, 1)
	} else {
		// change member on follower
		err := updatePartitionFollower(ep, data, nodeAddr)
		if err != nil {
			log.LogErrorf("updatePartitionFollower fail, PartitionID(%v)", ep.PartitionID)
			return
		}

		atomic.AddInt32(count, 1)
	}
}

func updatePartitionFollower(ep *EcPartition, data []byte, nodeAddr string) error {
	request := repl.NewPacket()
	request.ReqID = proto.GenerateRequestID()
	request.PartitionID = ep.PartitionID
	request.Opcode = proto.OpUpdateEcDataPartition
	request.Data = data
	request.Size = uint32(len(data))
	request.CRC = crc32.ChecksumIEEE(data)
	err := DoRequest(request, nodeAddr, proto.ReadDeadlineTime)
	if err != nil || request.ResultCode != proto.OpOk {
		return errors.NewErrorf("request OpUpdateEcDataPartition fail. node(%v) resultCode(%v) err:%v", nodeAddr, request.ResultCode, err)
	}

	return nil
}

func (ep *EcPartition) listExtents(i int, nodeAddr string, e *EcNode, wg *sync.WaitGroup, extentInfos []*extentInfoEcNode) {
	defer wg.Done()
	if nodeAddr == e.localServerAddr {
		list, err := ep.listExtentsLocal()
		if err != nil {
			log.LogErrorf("listExtentsLocal fail, PartitionID(%v), err:%v", ep.PartitionID, err)
			return
		}

		extentInfos[i] = ep.convertToExtentInfosEcNode(list, nodeAddr)
	} else {
		list, err := ep.listExtentsFollower(nodeAddr)
		if err != nil {
			log.LogErrorf("listExtentsFollower fail, PartitionID(%v), err:%v", ep.PartitionID, err)
			return
		}

		extentInfos[i] = ep.convertToExtentInfosEcNode(list, nodeAddr)
	}
}

func (ep *EcPartition) listExtentsLocal() (fInfoList []*storage.ExtentInfo, err error) {
	store := ep.ExtentStore()
	fInfoList, _, err = store.GetAllWatermarks(storage.NormalExtentFilter())
	return fInfoList, err
}

func (ep *EcPartition) convertToExtentInfosEcNode(extentInfoList []*storage.ExtentInfo, nodeAddr string) *extentInfoEcNode {
	p := &extentInfoEcNode{
		nodeAddr:      nodeAddr,
		partitionId:   ep.PartitionID,
		extentInfoMap: make(map[uint64]*storage.ExtentInfo),
	}

	maxExtentId := uint64(0)
	for _, file := range extentInfoList {
		extentId := file.FileID
		p.extentInfoMap[extentId] = file
		if extentId > maxExtentId {
			maxExtentId = extentId
		}
	}

	p.maxExtentId = maxExtentId
	return p
}

func (ep *EcPartition) updatePartitionAllEcNode(e *EcNode, r *proto.ChangeEcPartitionMembersRequest, data []byte) error {
	count := int32(0)
	wg := sync.WaitGroup{}
	wg.Add(len(r.Hosts))
	for _, host := range r.Hosts {
		go ep.updatePartition(host, data, e, &wg, &count)
	}
	wg.Wait()

	if count != int32(len(r.Hosts)) {
		return errors.NewErrorf("updatePartitionAllEcNode fail, PartitionID[%v]", ep.PartitionID)
	}

	return nil
}

func (ep *EcPartition) repairExtentData(ec *EcNode, info *storage.ExtentInfo) error {
	ecStripe, _ := NewEcStripe(ec, ep, info.FileID)
	extentFileOffset := uint64(0)
	for {
		if extentFileOffset >= info.Size {
			break
		}

		_, err := ecStripe.repairStripeData(extentFileOffset, ep.StripeUnitSize)
		if err != nil {
			return err
		}

		extentFileOffset += ep.StripeUnitSize
	}

	return nil
}

func (ep *EcPartition) listExtentsFollower(nodeAddr string) ([]*storage.ExtentInfo, error) {
	request := repl.NewPacket()
	request.ReqID = proto.GenerateRequestID()
	request.PartitionID = ep.PartitionID
	request.Opcode = proto.OpGetAllWatermarks
	err := DoRequest(request, nodeAddr, proto.ReadDeadlineTime)
	if err != nil {
		return nil, errors.NewErrorf("request OpGetAllWatermarks fail. node(%v) err:%v", nodeAddr, err)
	}

	if request.ResultCode != proto.OpOk || len(request.Data) == 0 {
		if len(request.Data) > 0 {
			return nil, errors.NewErrorf("OpGetAllWatermarks node(%v) resultCode(%v) data:%v", nodeAddr, request.ResultCode, string(request.Data))
		} else {
			return nil, errors.NewErrorf("OpGetAllWatermarks node(%v) resultCode(%v)", nodeAddr, request.ResultCode)
		}
	}

	var list []*storage.ExtentInfo
	err = json.Unmarshal(request.Data, &list)
	if err != nil {
		return nil, errors.NewErrorf("unmarshal ExtentInfo fail. data:%v err:%v", string(request.Data), err)
	}

	return list, nil
}

func (ep *EcPartition) repairPartitionData(ec *EcNode, maxExtentInfoEcNode *extentInfoEcNode, extentInfoEcNodeList []*extentInfoEcNode) {
	log.LogDebugf("repairPartitionData PartitionID(%v) node(%v) extent size(%v)",
		ep.PartitionID, maxExtentInfoEcNode.nodeAddr, len(maxExtentInfoEcNode.extentInfoMap))

	maxExtentInfoMap := maxExtentInfoEcNode.extentInfoMap
	for extentId := range maxExtentInfoMap {
		extentInfo := maxExtentInfoMap[extentId]
		if extentInfo == nil {
			log.LogWarnf("PartitionID(%v) node(%v) cannot repair this extent, ExtentID(%v) not exist ",
				ep.PartitionID, maxExtentInfoEcNode.nodeAddr, extentId)
			continue
		}

		count := 0
		for _, extentInfoEcNode := range extentInfoEcNodeList {
			otherExtentInfo, ok := extentInfoEcNode.extentInfoMap[extentId]
			if ok && otherExtentInfo != nil && extentInfo.Size == otherExtentInfo.Size {
				count += 1
			}
		}

		if canRepair(count, ep, extentInfo) {
			err := ep.repairExtentData(ec, extentInfo)
			if err != nil {
				log.LogErrorf("fail to repair this extent, PartitionID(%v) ExtentID(%v) extentInfo(%v) err:%v",
					maxExtentInfoEcNode.partitionId, extentId, extentInfo.String(), err)
			}
		} else {
			log.LogWarnf("cannot repair this extent, PartitionID(%v) ExtentID(%v) extentInfo(%v)",
				maxExtentInfoEcNode.partitionId, extentId, extentInfo.String())
		}
	}
}

func canRepair(count int, ep *EcPartition, extentInfo *storage.ExtentInfo) bool {
	if count < int(ep.DataNodeNum) {
		// cannot repair, because of no enough ecNode with this extent
		return false
	}

	if extentInfo.Size == 0 {
		// cannot repair, this extent is empty(size is zero)
		return false
	}

	if count == int(ep.DataNodeNum+ep.ParityNodeNum) {
		// no need repair, all EcNode extent are same
		return false
	}

	return true
}
