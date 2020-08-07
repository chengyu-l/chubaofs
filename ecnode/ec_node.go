package ecnode

import (
	"github.com/chubaofs/chubaofs/proto"
	"github.com/chubaofs/chubaofs/repl"
	"github.com/chubaofs/chubaofs/storage"
	"github.com/chubaofs/chubaofs/util/log"
	"sync"
	"sync/atomic"
	"time"
)

type extentInfoEcNode struct {
	extentInfoMap map[uint64]*storage.ExtentInfo
	nodeAddr      string
	partitionId   uint64
	maxExtentId   uint64
}

func (e *EcNode) createExtentOnFollower(ep *EcPartition, extentId uint64) bool {
	var count int32
	wg := sync.WaitGroup{}
	wg.Add(len(ep.Hosts))
	for _, host := range ep.Hosts {
		go func(addr string) {
			defer wg.Done()

			if addr == e.localServerAddr {
				// create extent local
				if err := ep.ExtentStore().Create(extentId); err != nil {
					log.LogErrorf("fail to create extent local. node(%v) partitionID(%v) extentID(%d) error:%v",
						addr, ep.PartitionID, extentId, err)
					return
				}
			} else {
				// create extent from remote node
				request := NewCreateExtent(ep.PartitionID, extentId)
				err := DoRequest(request, addr, proto.ReadDeadlineTime)
				if err != nil || request.ResultCode != proto.OpOk {
					log.LogErrorf("fail to create extent from remote. node(%v) partitionID(%v) extentID(%d) error:%v ",
						addr, ep.PartitionID, extentId, err)
					return
				}
			}

			atomic.AddInt32(&count, 1)
		}(host)
	}

	wg.Wait()
	log.LogDebugf("codecnode->ecnode create extent[%v] success, success count(%v) total count(%v)", extentId, count, len(ep.Hosts))
	if int(count) == len(ep.Hosts) {
		return true
	} else {
		return false
	}
}

func NewCreateExtent(partitionID uint64, extentID uint64) (p *repl.Packet) {
	p = new(repl.Packet)
	p.ExtentID = extentID
	p.PartitionID = partitionID
	p.Magic = proto.ProtoMagic
	p.Opcode = proto.OpCreateExtent
	p.ReqID = proto.GenerateRequestID()
	p.StartT = time.Now().UnixNano()
	return
}
