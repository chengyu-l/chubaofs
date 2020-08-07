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
	"hash/crc32"
	"net"
	"strconv"
	"time"

	"github.com/chubaofs/chubaofs/proto"
	"github.com/chubaofs/chubaofs/repl"
	"github.com/chubaofs/chubaofs/storage"
	"github.com/chubaofs/chubaofs/util/errors"
	"github.com/chubaofs/chubaofs/util/exporter"
	"github.com/chubaofs/chubaofs/util/log"
)

func (e *EcNode) OperatePacket(p *repl.Packet, c *net.TCPConn) (err error) {
	sz := p.Size
	tpObject := exporter.NewTPCnt(p.GetOpMsg())
	start := time.Now().UnixNano()
	defer func() {
		resultSize := p.Size
		p.Size = sz
		if p.IsErrPacket() {
			err = fmt.Errorf("op(%v) error(%v)", p.GetOpMsg(), string(p.Data[:resultSize]))
			logContent := fmt.Sprintf("action[OperatePacket] %v",
				p.LogMessage(p.GetOpMsg(), c.RemoteAddr().String(), start, err))
			log.LogErrorf(logContent)
		} else {
			logContent := fmt.Sprintf("action[OperatePacket] %v.",
				p.LogMessage(p.GetOpMsg(), c.RemoteAddr().String(), start, nil))
			switch p.Opcode {
			default:
				log.LogInfo(logContent)
			}
		}
		p.Size = resultSize
		tpObject.Set(err)
	}()

	switch p.Opcode {
	case proto.OpCreateEcDataPartition:
		e.handlePacketToCreateEcPartition(p)
	case proto.OpUpdateEcDataPartition:
		e.handleUpdateEcPartition(p)
	case proto.OpEcNodeHeartbeat:
		e.handleHeartbeatPacket(p)
	case proto.OpCreateExtent:
		e.handlePacketToCreateExtent(p)
	case proto.OpWrite:
		e.handleWritePacket(p)
	case proto.OpRead:
		e.handleReadPacket(p, c)
	case proto.OpStreamRead, proto.OpStreamFollowerRead:
		e.handleStreamReadPacket(p, c)
	case proto.OpNotifyReplicasToRepair:
		e.handleRepairWrite(p)
	case proto.OpChangeEcPartitionMembers:
		e.handleChangeMember(p)
	case proto.OpGetAllWatermarks:
		e.handleGetAllWatermarks(p)
	case proto.OpEcExtentValidate:
		e.handleEcExtentValidate(p)
	case proto.OpEcExtentRepair:
		e.handleEcExtentRepair(p)
	default:
		p.PackErrorBody(repl.ErrorUnknownOp.Error(), repl.ErrorUnknownOp.Error()+strconv.Itoa(int(p.Opcode)))
	}
	return
}

// Handle OpCreateEcDataPartition to create new EcPartition
func (e *EcNode) handlePacketToCreateEcPartition(p *repl.Packet) {
	var (
		err error
		ep  *EcPartition
	)

	log.LogDebugf("CreateEcPartition:%v", string(p.Data))
	task := &proto.AdminTask{}
	err = json.Unmarshal(p.Data, task)
	if err != nil {
		log.LogErrorf("cannnot unmashal adminTask")
		err = fmt.Errorf("cannnot unmashal adminTask")
		return
	}
	if task.OpCode != proto.OpCreateEcDataPartition {
		log.LogErrorf("error unavaliable opcode")
		err = fmt.Errorf("from master Task(%v) failed, error unavaliable opcode(%v), expected opcode(%v)",
			task.ToString(), task.OpCode, proto.OpCreateEcDataPartition)
		return
	}

	request := &proto.CreateEcPartitionRequest{}
	bytes, err := json.Marshal(task.Request)
	err = json.Unmarshal(bytes, request)
	if err != nil {
		log.LogErrorf("cannot convert to CreateEcPartition")
		err = fmt.Errorf("from master Task(%v) cannot convert to CreateEcPartition", task.ToString())
		return
	}

	ep, err = e.space.CreatePartition(request)
	if err != nil {
		log.LogErrorf("cannot create Partition err(%v)", err)
		err = fmt.Errorf("from master Task(%v) cannot create Partition err(%v)", task.ToString(), err)
		return
	}
	p.PacketOkWithBody([]byte(ep.Disk().Path))

	return
}

// Handle OpHeartbeat packet
func (e *EcNode) handleHeartbeatPacket(p *repl.Packet) {
	task := &proto.AdminTask{}
	err := json.Unmarshal(p.Data, task)
	defer func() {
		if err != nil {
			p.PackErrorBody(ActionHeartbeat, err.Error())
		} else {
			p.PacketOkReply()
		}
	}()

	if err != nil {
		return
	}

	go func() {
		response := &proto.EcNodeHeartbeatResponse{
			Status: proto.TaskSucceeds,
		}
		e.buildHeartbeatResponse(response)

		if task.OpCode == proto.OpEcNodeHeartbeat {
			response.Status = proto.TaskSucceeds
		} else {
			response.Status = proto.TaskFailed
			err = fmt.Errorf("illegal opcode")
			response.Result = err.Error()
		}
		task.Response = *response

		log.LogDebugf(fmt.Sprintf("%v", task))

		err = MasterClient.NodeAPI().ResponseEcNodeTask(task)
		if err != nil {
			log.LogErrorf(err.Error())
			return
		}
	}()
}

// Handle OpCreateExtent packet.
func (e *EcNode) handlePacketToCreateExtent(p *repl.Packet) {
	var err error
	defer func() {
		if err != nil {
			p.PackErrorBody(ActionCreateExtent, err.Error())
		} else {
			p.PacketOkReply()
		}
	}()

	ep := p.Object.(*EcPartition)
	if ep.Available() <= 0 || ep.disk.Status == proto.ReadOnly || ep.IsRejectWrite() {
		err = storage.NoSpaceError
		return
	} else if ep.disk.Status == proto.Unavailable {
		err = storage.BrokenDiskError
		return
	}

	extentId := p.ExtentID
	if extentId == 0 {
		log.LogDebugf("codecnode->ecnode createExtent, packet:%v", p)
		// codecnode -> ecnode for create extent
		extentId, err = ep.GetNewExtentId()
		if err != nil {
			log.LogDebugf("codecnode->ecnode GetNewExtentId fail, error:%v", err)
			err = storage.NoBrokenExtentError
			return
		}

		ok := e.createExtentOnFollower(ep, extentId)
		if !ok {
			log.LogDebugf("codecnode->ecnode createExtentOnFollower fail, newExtentId:%v", extentId)
			err = storage.BrokenExtentError
			return
		}

		p.ExtentID = extentId
		return
	}

	log.LogDebugf("ecnode->ecnode CreateExtent, packet:%v", p)
	// master ecnode -> follower ecnode for create extent
	err = ep.ExtentStore().Create(extentId)
	return
}

// Handle OpWrite packet.
func (e *EcNode) handleWritePacket(p *repl.Packet) {
	var err error
	defer func() {
		if err != nil {
			p.PackErrorBody(ActionWrite, err.Error())
		} else {
			p.PacketOkReply()
		}
	}()

	ep := p.Object.(*EcPartition)
	if ep.Available() <= 0 || ep.disk.Status == proto.ReadOnly || ep.IsRejectWrite() {
		err = storage.NoSpaceError
		return
	} else if ep.disk.Status == proto.Unavailable {
		err = storage.BrokenDiskError
		return
	}

	// we only allow write by one stripe unit
	if uint64(p.Size)%ep.StripeUnitSize != 0 && uint64(p.Size) > ep.ExtentFileSize {
		err = errors.New("invalid size, must be in range (StripeUnitSize, ExtentFileSize)")
		return
	}

	ecStripe, _ := NewEcStripe(e, ep, p.ExtentID)
	err = ecStripe.writeToExtent(p, storage.AppendWriteType)
	e.incDiskErrCnt(p.PartitionID, err, WriteFlag)
}

// Handle OpStreamFollowerRead & OpStreamRead packet.
func (e *EcNode) handleStreamReadPacket(p *repl.Packet, connect net.Conn) {
	var err error
	defer func() {
		if err != nil {
			log.LogErrorf("StreamRead fail, err:%v", err)
			p.PackErrorBody(ActionStreamRead, err.Error())
			err = p.WriteToConn(connect)
			if err != nil {
				log.LogDebugf("StreamRead write to client fail, packet:%v err:%v", p, err)
			}
		}
	}()

	ep := p.Object.(*EcPartition)
	ecStripe, _ := NewEcStripe(e, ep, p.ExtentID)
	readSize := uint64(p.Size)
	extentOffset := uint64(p.ExtentOffset)
	for {
		if readSize <= 0 {
			break
		}

		nodeAddr := ecStripe.calcNode(extentOffset)
		extentFileOffset := ecStripe.calcExtentFileOffset(extentOffset)
		curReadSize := ecStripe.calcCanReadSize(extentOffset, readSize)
		data, crc, err := ecStripe.readStripeData(nodeAddr, extentFileOffset, curReadSize)
		log.LogDebugf("StreamRead reply packet: PartitionID(%v) ExtentID(%v) nodeAddr(%v) offset(%v) readSize(%v) actual dataSize(%v) crc(%v) err:%v",
			p.PartitionID, p.ExtentID, nodeAddr, extentFileOffset, curReadSize, len(data), crc, err)
		if err != nil || len(data) == 0 {
			// repair read process
			data, crc, err = repairReadStripeProcess(ecStripe, extentFileOffset, curReadSize, p, nodeAddr)
			if err != nil || len(data) == 0 {
				err = errors.NewErrorf("StreamRead repairReadStripeProcess fail, ReqID(%v) PartitionID(%v) ExtentID(%v) ExtentOffset(%v) nodeAddr(%v) extentFileOffset(%v) curReadSize(%v) dataSize(%v). err:%v",
					p.ReqID, p.PartitionID, p.ExtentID, p.ExtentOffset, nodeAddr, extentFileOffset, curReadSize, len(data), err)
				return
			}
		}

		reply := NewReadReply(p)
		reply.ExtentOffset = int64(extentOffset)
		reply.Data = data
		reply.Size = uint32(curReadSize)
		reply.CRC = crc
		reply.ResultCode = proto.OpOk
		if err = reply.WriteToConn(connect); err != nil {
			msg := fmt.Sprintf("StreamRead write to client fail. ReqID(%v) PartitionID(%v) ExtentID(%v) ExtentOffset[%v] Size[%v] err:%v",
				reply.ReqID, reply.PartitionID, reply.ExtentID, reply.ExtentOffset, reply.Size, err)
			err = errors.New(msg)
			return
		}

		extentOffset += curReadSize
		readSize -= curReadSize
	}

	p.PacketOkReply()
}

func repairReadStripeProcess(ecStripe *ecStripe, extentFileOffset uint64, curReadSize uint64, p *repl.Packet, nodeAddr string, ) (data []byte, crc uint32, err error) {
	repairData, err := ecStripe.repairStripeData(extentFileOffset, curReadSize)
	if err != nil {
		return nil, 0, err
	}

	for i := 0; i < len(ecStripe.hosts); i++ {
		if ecStripe.hosts[i] == nodeAddr && len(repairData[i]) > 0 {
			data = repairData[i]
			crc = crc32.ChecksumIEEE(data)
			return data, crc, nil
		}
	}

	return nil, 0, errors.NewErrorf("repairStripeData is empty")
}

// Handle OpRead packet.
func (e *EcNode) handleReadPacket(p *repl.Packet, c *net.TCPConn) {
	var err error
	defer func() {
		if err != nil {
			p.PackErrorBody(ActionRead, err.Error())
			err = p.WriteToConn(c)
			if err != nil {
				log.LogDebugf("StreamRead write to client fail, packet:%v err:%v", p, err)
			}
		}
	}()

	partition := p.Object.(*EcPartition)
	// only allow read by one stripe unit
	if uint64(p.Size) != partition.StripeUnitSize {
		err = errors.New("invalid read size, must be equals StripeUnitSize")
		return
	}

	ecStripe, _ := NewEcStripe(e, partition, p.ExtentID)
	data := make([]byte, p.Size)
	tpObject := exporter.NewTPCnt(p.GetOpMsg())
	crc, err := ecStripe.readExtentFile(uint64(p.ExtentOffset), uint64(p.Size), data)
	partition.checkIsDiskError(err)
	tpObject.Set(err)
	if err != nil {
		log.LogErrorf("fail to read extent. ReqID(%v) PartitionID(%v) ExtentId(%v) ExtentOffset[%v] Size[%v] err:%v",
			p.ReqID, p.PartitionID, p.ExtentID, p.ExtentOffset, p.Size, err)
		data = nil
		return
	}

	p.Data = data
	p.CRC = crc
	p.ResultCode = proto.OpOk
	log.LogDebugf("read extent success, ReqID(%v) PartitionID(%v) ExtentId(%v)", p.ReqID, p.PartitionID, p.ExtentID)
	err = p.WriteToConn(c)
	if err != nil {
		log.LogErrorf("write reply packet error. ReqID(%v) PartitionID(%v) ExtentId(%v) ExtentOffset[%v] Size[%v] Crc[%v] err:%v",
			p.ReqID, p.PartitionID, p.ExtentID, p.ExtentOffset, p.Size, p.CRC, err)
		return
	}

	p.PacketOkReply()
}

// Handle OpGetAllWatermarks packet.
func (e *EcNode) handleGetAllWatermarks(p *repl.Packet) {
	var err error
	defer func() {
		if err != nil {
			log.LogErrorf("%v packet:%v error: %v", ActionGetAllExtentWatermarks, p, err)
			p.PackErrorBody(ActionGetAllExtentWatermarks, err.Error())
		}
	}()

	ep := e.space.Partition(p.PartitionID)
	if ep == nil {
		err = proto.ErrNoAvailEcPartition
		return
	}

	extentList, err := ep.listExtentsLocal()
	if err != nil {
		err = errors.NewErrorf("list extent fail, err:%v", err)
		return
	}

	buf, err := json.Marshal(extentList)
	if err != nil {
		err = errors.NewErrorf("marshal extent list fail, err:%v", err)
		return
	}

	p.PacketOkWithBody(buf)
}

// Handle OpChangeEcPartitionMembers packet.
func (e *EcNode) handleChangeMember(p *repl.Packet) {
	var err error
	defer func() {
		if err != nil {
			log.LogErrorf("%v fail. packet:%v data:%v err:%v", ActionChangeMember, p, string(p.Data), err)
			p.PackErrorBody(ActionChangeMember, err.Error())
		} else {
			p.PacketOkReply()
		}
	}()

	ep := e.space.Partition(p.PartitionID)
	if ep == nil {
		err = proto.ErrNoAvailEcPartition
		return
	}

	log.LogDebugf("change member PartitionID(%v) data:%v", p.PartitionID, string(p.Data))
	r, err := getChangeEcPartitionMemberRequest(p.Data)
	if err != nil {
		return
	}

	if len(r.Hosts) != int(ep.DataNodeNum+ep.ParityNodeNum) {
		err = errors.NewErrorf("no enough new hosts for change member, err:%v", err)
		return
	}

	err = ep.changeMember(e, r, p.Data)
}

// Handle OpNotifyReplicasToRepair packet.
func (e *EcNode) handleRepairWrite(p *repl.Packet) {
	var err error
	defer func() {
		if err != nil {
			p.PackErrorBody(ActionReplicasToRepair, err.Error())
		} else {
			p.PacketOkReply()
		}
	}()

	log.LogDebugf("RepairWrite PartitionID(%v) ExtentId(%v) offset(%v) size(%v) data len(%v), crc(%v)",
		p.PartitionID, p.ExtentID, p.ExtentOffset, p.Size, len(p.Data), p.CRC)

	ep := p.Object.(*EcPartition)
	if uint64(p.Size) != ep.StripeUnitSize {
		err = errors.NewErrorf("data size must be equals a StripeUnitSize(%v)", ep.StripeUnitSize)
		return
	}

	store := ep.extentStore
	if !store.HasExtent(p.ExtentID) {
		// create ExtentFile
		err = store.Create(p.ExtentID)
		if err != nil {
			err = errors.NewErrorf("RepairWrite createExtent fail, err:%v", err)
			return
		}
	}

	ecStripe, _ := NewEcStripe(e, ep, p.ExtentID)
	err = ecStripe.writeToExtent(p, storage.RandomWriteType)
	e.incDiskErrCnt(p.PartitionID, err, WriteFlag)
}

// Handle OpUpdateEcDataPartition packet.
func (e *EcNode) handleUpdateEcPartition(p *repl.Packet) {
	var err error
	defer func() {
		if err != nil {
			log.LogErrorf("UpdateEcPartition fail. PartitionID(%v) err:%v", p.PartitionID, err)
			p.PackErrorBody(ActionUpdateEcPartition, err.Error())
		} else {
			p.PacketOkReply()
		}
	}()

	ep := e.space.Partition(p.PartitionID)
	if ep == nil {
		err = proto.ErrNoAvailEcPartition
		return
	}

	err = ep.updatePartitionLocal(p.Data)
}

// Handle OpEcExtentValidate packet.
func (e *EcNode) handleEcExtentValidate(p *repl.Packet) {
	var err error
	defer func() {
		if err != nil {
			log.LogErrorf("EcExtentValidate fail, err:%v", err)
			p.PackErrorBody(ActionValidateEcPartition, err.Error())
		}
	}()

	ep := e.space.Partition(p.PartitionID)
	if ep == nil {
		err = proto.ErrNoAvailEcPartition
		return
	}

	ecStripe, _ := NewEcStripe(e, ep, p.ExtentID)
	extentInfo, err := ecStripe.validate()
	if err != nil {
		return
	}

	marshal, err := json.Marshal(extentInfo)
	if err != nil {
		err = errors.NewErrorf("EcExtentValidate success, but marshal extentInfo fail")
		return
	}

	p.PacketOkWithBody(marshal)
}

func (e *EcNode) handleEcExtentRepair(p *repl.Packet) {
	var err error
	defer func() {
		if err != nil {
			log.LogErrorf("EcExtentRepair fail, err:%v", err)
			p.PackErrorBody(ActionRepairRead, err.Error())
		}
	}()

	log.LogDebugf("EcExtentRepair: ReqID(%v) PartitionID(%v) ExtentID(%v)", p.ReqID, p.PartitionID, p.ExtentID)
	ep := p.Object.(*EcPartition)
	extentInfo, err := ep.GetExtentInfo(p.ExtentID)
	if err != nil {
		return
	}

	err = ep.repairExtentData(e, extentInfo)
	if err != nil {
		return
	}

	p.PacketOkReply()
}
