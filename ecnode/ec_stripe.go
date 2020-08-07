package ecnode

import (
	"fmt"
	"github.com/chubaofs/chubaofs/codecnode"
	"github.com/chubaofs/chubaofs/proto"
	"github.com/chubaofs/chubaofs/repl"
	"github.com/chubaofs/chubaofs/storage"
	"github.com/chubaofs/chubaofs/util"
	"github.com/chubaofs/chubaofs/util/errors"
	"github.com/chubaofs/chubaofs/util/log"
	"github.com/shopspring/decimal"
	"hash/crc32"
	"sync"
)

type ecStripe struct {
	e               *EcNode
	ep              *EcPartition
	partitionId     uint64
	extentID        uint64
	dataSize        uint64
	paritySize      uint64
	localServerAddr string
	hosts           []string
	coder           *codecnode.EcHandler
}

func NewEcStripe(e *EcNode, ep *EcPartition, extentID uint64) (*ecStripe, error) {
	nodeNum := int(ep.DataNodeNum + ep.ParityNodeNum)
	nodeIndex := int(extentID % uint64(nodeNum))
	hosts := make([]string, nodeNum)
	for i := 0; i < nodeNum; i++ {
		if nodeIndex >= nodeNum {
			nodeIndex = 0
		}

		hosts[i] = ep.Hosts[nodeIndex]
		nodeIndex = nodeIndex + 1
	}

	coder, err := codecnode.NewEcCoder(int(ep.StripeUnitSize), int(ep.DataNodeNum), int(ep.ParityNodeNum))
	if err != nil {
		log.LogErrorf("NewEcCoder fail. err:%v", err)
		return nil, errors.NewErrorf("NewEcCoder fail. err:%v", err)
	}

	return &ecStripe{
		e:               e,
		ep:              ep,
		partitionId:     ep.PartitionID,
		extentID:        extentID,
		dataSize:        ep.StripeUnitSize * uint64(ep.DataNodeNum),
		paritySize:      ep.StripeUnitSize * uint64(ep.ParityNodeNum),
		localServerAddr: e.localServerAddr,
		hosts:           hosts,
		coder:           coder,
	}, nil
}

func (ee *ecStripe) readStripe(nodeAddr string, offset uint64, curReadSize uint64, wg *sync.WaitGroup) (data []byte) {
	defer wg.Done()

	if nodeAddr == ee.localServerAddr {
		// read local
		data = make([]byte, ee.ep.StripeUnitSize)
		_, err := ee.readExtentFile(offset, curReadSize, data)
		if err != nil {
			log.LogWarnf("read local[%v] fail. partitionId[%v] extentId[%v] offset[%v] err:%v",
				nodeAddr, ee.partitionId, ee.extentID, offset, err)
			data = nil
			return
		}
	} else {
		// read from remote node
		request := repl.NewExtentStripeRead(ee.partitionId, ee.extentID, offset, curReadSize)
		err := DoRequest(request, nodeAddr, proto.ReadDeadlineTime)
		if err != nil || request.ResultCode != proto.OpOk {
			log.LogWarnf("read from remote[%v] fail. partitionId[%v] extentId[%v] offset[%v] size[%v] crc[%v] err:%v",
				nodeAddr, ee.partitionId, ee.extentID, offset, request.Size, request.CRC, err)
			return
		}

		data = request.Data
	}

	return
}

func (ee *ecStripe) readExtentFile(offset uint64, size uint64, data []byte) (uint32, error) {
	if ee.ep == nil {
		return 0, proto.ErrNoAvailEcPartition
	}

	store := ee.ep.extentStore
	if store == nil {
		return 0, proto.ErrNoAvailEcPartition
	}

	return store.Read(ee.extentID, int64(offset), int64(size), data, false)
}

func (ee *ecStripe) readStripeData(nodeAddr string, extentFileOffset, curReadSize uint64) (data []byte, crc uint32, err error) {
	if nodeAddr == ee.localServerAddr {
		// read local
		data = make([]byte, curReadSize)
		crc, err = ee.readExtentFile(extentFileOffset, curReadSize, data)
		if err != nil {
			log.LogErrorf("read local[%v] fail. partitionId[%v] extentId[%v] offset[%v] size[%v] err:%v",
				nodeAddr, ee.partitionId, ee.extentID, extentFileOffset, curReadSize, err)
			data = nil
			return
		}

		log.LogDebugf("read local[%v]. partitionId[%v] extentId[%v] offset[%v] size[%v]",
			nodeAddr, ee.partitionId, ee.extentID, extentFileOffset, curReadSize)
	} else {
		// read from remote node
		request := repl.NewExtentStripeRead(ee.partitionId, ee.extentID, extentFileOffset, curReadSize)
		err = DoRequest(request, nodeAddr, proto.ReadDeadlineTime)
		if err != nil || request.ResultCode != proto.OpOk {
			log.LogErrorf("read from remote[%v] fail. packet:%v offset[%v] size[%v] crc[%v] err:%v",
				nodeAddr, request, extentFileOffset, request.Size, request.CRC, err)
			return
		}

		log.LogDebugf("read from remote[%v]. packet:%v offset[%v] size[%v] crc[%v]",
			nodeAddr, request, extentFileOffset, curReadSize, request.CRC)
		data = request.Data
		crc = request.CRC
	}

	return
}

func (ee *ecStripe) calcNode(extentOffset uint64) string {
	stripeUnitSizeDecimal := decimal.NewFromInt(int64(ee.ep.StripeUnitSize))
	extentOffsetDecimal := decimal.NewFromInt(int64(extentOffset))
	div := extentOffsetDecimal.Div(stripeUnitSizeDecimal).Floor()
	index := div.Mod(decimal.NewFromInt(int64(ee.ep.DataNodeNum))).IntPart()
	if index == 0 {
		fmt.Println(extentOffset)
	}
	return ee.hosts[index]
}

func (ee *ecStripe) calcExtentFileOffset(extentOffset uint64) uint64 {
	stripeUnitSizeDecimal := decimal.NewFromInt(int64(ee.ep.StripeUnitSize))
	extentOffsetDecimal := decimal.NewFromInt(int64(extentOffset))
	div := extentOffsetDecimal.Div(stripeUnitSizeDecimal).Floor()
	stripeN := div.Div(decimal.NewFromInt(int64(ee.ep.DataNodeNum))).Floor().IntPart()
	offset := extentOffsetDecimal.Mod(stripeUnitSizeDecimal).IntPart()
	return uint64(stripeN)*ee.ep.StripeUnitSize + uint64(offset)
}

func (ee *ecStripe) calcCanReadSize(extentOffset uint64, canReadSize uint64) uint64 {
	canReadSizeInStripeUnit := ee.ep.StripeUnitSize - extentOffset%ee.ep.StripeUnitSize
	if canReadSize < canReadSizeInStripeUnit {
		return canReadSize
	} else {
		return canReadSizeInStripeUnit
	}
}

func (ee *ecStripe) reconstructStripeData(extentFileOffset, curReadSize uint64) (data [][]byte, validDataBitmap []byte, err error) {
	data = ee.readStripeAllEcNode(extentFileOffset, curReadSize)
	validDataBitmap = make([]byte, len(ee.hosts))
	validDataCount := 0
	for i := 0; i < len(validDataBitmap); i++ {
		if len(data[i]) > 0 {
			validDataCount += 1
			validDataBitmap[i] = 1
		}
	}

	if validDataCount < int(ee.ep.DataNodeNum) {
		return nil, nil, errors.NewErrorf("PartitionID(%v) ExtentID(%v) not enough data to reconstruct. validDataCount(%v) DataNodeNum(%v)",
			ee.partitionId, ee.extentID, validDataCount, ee.ep.DataNodeNum)
	}

	verify, _ := ee.coder.Verify(data)
	if verify {
		return data, validDataBitmap, nil
	}

	// reconstruct data only support the situation that the EcExtent file does not exist.
	// If the EcExtent file exist, but it have silent data corruption or bit rot problem, it cannot be repaired.
	// Because we use the reedsolomon library that is not support this action
	err = ee.reconstructData(data)
	if err != nil {
		return nil, nil, errors.NewErrorf("reconstruct data fail, PartitionID(%v) ExtentID(%v) node(%v) offset(%v) validDataBitmap(%v), err:%v",
			ee.partitionId, ee.extentID, ee.hosts, extentFileOffset, validDataBitmap, err)
	}

	return data, validDataBitmap, nil
}

func (ee *ecStripe) repairStripeData(extentFileOffset, curReadSize uint64) ([][]byte, error) {
	data, validDataBitmap, err := ee.reconstructStripeData(extentFileOffset, curReadSize)
	if err != nil {
		return nil, err
	}

	log.LogDebugf("reconstruct data success, PartitionID(%v) ExtentID(%v) node(%v) offset(%v) validDataBitmap(%v)",
		ee.partitionId, ee.extentID, ee.hosts, extentFileOffset, validDataBitmap)
	go ee.writeBackReconstructData(data, validDataBitmap, extentFileOffset)
	return data, nil
}

func (ee *ecStripe) readStripeAllEcNode(extentFileOffset uint64, curReadSize uint64) [][]byte {
	ecNodeNum := ee.ep.DataNodeNum + ee.ep.ParityNodeNum
	data := make([][]byte, ecNodeNum)
	wg := sync.WaitGroup{}
	wg.Add(int(ecNodeNum))
	for i, nodeAddr := range ee.hosts {
		go func(innerData [][]byte, index int, innerNodeAddr string) {
			innerData[index] = ee.readStripe(innerNodeAddr, extentFileOffset, curReadSize, &wg)
			log.LogDebugf("readStripe node(%v) PartitionID(%v) ExtentID(%v) offset(%v) size(%v) len(%v)",
				innerNodeAddr, ee.partitionId, ee.extentID, extentFileOffset, curReadSize, len(innerData[index]))
		}(data, i, nodeAddr)
	}

	wg.Wait()
	return data
}

func (ee *ecStripe) reconstructData(pBytes [][]byte) error {
	log.LogDebugf("reconstructData, partitionID(%v) extentID(%v)", ee.partitionId, ee.extentID)
	ep := ee.ep
	coder, err := codecnode.NewEcCoder(int(ep.StripeUnitSize), int(ep.DataNodeNum), int(ep.ParityNodeNum))
	if err != nil {
		return errors.New(fmt.Sprintf("NewEcCoder error:%s", err))
	}

	return coder.Reconstruct(pBytes)
}

// only write back the data that the EcExtent file does not exist.
// Because we use the reedsolomon library that only support handle this situation.
func (ee *ecStripe) writeBackReconstructData(data [][]byte, validDataBitmap []byte, extentFileOffset uint64) {
	for i := 0; i < len(validDataBitmap); i++ {
		nodeAddr := ee.hosts[i]
		if validDataBitmap[i] == 1 {
			continue
		}

		request := repl.NewPacket()
		request.ExtentID = ee.extentID
		request.PartitionID = ee.partitionId
		request.ExtentOffset = int64(extentFileOffset)
		request.Size = uint32(len(data[i]))
		request.Data = data[i]
		request.CRC = crc32.ChecksumIEEE(data[i])
		request.Opcode = proto.OpNotifyReplicasToRepair
		request.ReqID = proto.GenerateRequestID()
		err := DoRequest(request, nodeAddr, proto.ReadDeadlineTime)
		// if err not nil, do nothing, only record log
		if err != nil || request.ResultCode != proto.OpOk {
			log.LogDebugf("write back reconstruct data. PartitionID(%v) ExtentID(%v) node(%v) offset(%v) size(%v), resultCode(%v) err:%v",
				ee.partitionId, ee.extentID, nodeAddr, extentFileOffset, len(data[i]), request.ResultCode, err)
		} else {
			log.LogDebugf("write back reconstruct data. PartitionID(%v) ExtentID(%v) node(%v) offset(%v) size(%v)",
				ee.partitionId, ee.extentID, nodeAddr, extentFileOffset, len(data[i]))
		}
	}
}

func (ee *ecStripe) validate() (extentInfo *storage.ExtentInfo, err error) {
	ep := ee.ep
	extentInfo, err = ep.ExtentStore().Watermark(ee.extentID)
	if err != nil || extentInfo == nil {
		err = fmt.Errorf("extent %v not exist", ee.extentID)
		return
	}

	coder, err := codecnode.NewEcCoder(int(ep.StripeUnitSize), int(ep.DataNodeNum), int(ep.ParityNodeNum))
	if err != nil || coder == nil {
		err = fmt.Errorf("NewEcCoder fail, err:%v", err)
		return
	}

	readSize := extentInfo.Size
	extentOffset := uint64(0)
	for {
		if readSize <= 0 {
			break
		}

		data := ee.readStripeAllEcNode(extentOffset, ep.StripeUnitSize)
		validDataCount := 0
		for i := 0; i < len(ee.hosts); i++ {
			if len(data[i]) > 0 {
				validDataCount += 1
			}
		}

		if validDataCount < len(ee.hosts) {
			err = errors.NewErrorf("not enough data to verify. Partition(%v) ExtentInfo(%v) validDataCount(%v) needDataCount(%v)",
				ee.partitionId, extentInfo.String(), validDataCount, len(ee.hosts))
			return extentInfo, err
		}

		verify, err := coder.Verify(data)
		if err != nil || !verify {
			err = fmt.Errorf("verify fail, Partition(%v) ExtentInfo(%v) extentFileOffset(%v) size(%v) verify(%v) err:%v",
				ee.partitionId, extentInfo.String(), extentOffset, ep.StripeUnitSize, verify, err)
			return extentInfo, err
		}

		readSize -= ep.StripeUnitSize
		extentOffset += ep.StripeUnitSize
	}

	return
}

func (ee *ecStripe) writeToExtent(p *repl.Packet, writeType int) (err error) {
	ep := ee.ep
	store := ep.extentStore
	if p.Size <= util.BlockSize {
		err = store.Write(p.ExtentID, p.ExtentOffset, int64(p.Size), p.Data, p.CRC, writeType, p.IsSyncWrite())
		ep.checkIsDiskError(err)
	} else {
		size := p.Size
		offset := 0
		for size > 0 {
			if size <= 0 {
				break
			}
			currSize := util.Min(int(size), util.BlockSize)
			data := p.Data[offset : offset+currSize]
			crc := crc32.ChecksumIEEE(data)
			err = store.Write(p.ExtentID, p.ExtentOffset+int64(offset), int64(currSize), data, crc, writeType, p.IsSyncWrite())
			ep.checkIsDiskError(err)
			if err != nil {
				break
			}
			size -= uint32(currSize)
			offset += currSize
		}
	}

	return err
}
