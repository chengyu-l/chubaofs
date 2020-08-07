package ecnode

import (
	"github.com/chubaofs/chubaofs/proto"
	"github.com/chubaofs/chubaofs/repl"
	"net"
	"os"
	"testing"
	"time"
)

func TestEcNode_CreateExtent(t *testing.T) {
	ec := newFakeEcNode(t, nil)
	defer os.RemoveAll(ec.path)
	ep := ec.fakeCreateECPartition(t, fakePartitionID)
	ec.fakeCreateExtent(ep, t)
}

func TestEcNode_handleWritePacket(t *testing.T) {
	ec := newFakeEcNode(t, nil)
	defer os.RemoveAll(ec.path)
	ep := ec.fakeCreateECPartition(t, fakePartitionID)
	ec.fakeCreateExtent(ep, t)

	fakeWriteExtent(ep, ec, t)
}

func TestEcNode_handleReadPacket(t *testing.T) {
	ec := newFakeEcNode(t, nil)
	defer os.RemoveAll(ec.path)
	ep := ec.fakeCreateECPartition(t, fakePartitionID)
	ec.fakeCreateExtent(ep, t)

	beforeCrc := ec.prepareTestData(t, ep)
	// read
	p := &repl.Packet{
		Object: ep,
		Packet: proto.Packet{
			Magic:       proto.ProtoMagic,
			ReqID:       proto.GenerateRequestID(),
			Opcode:      proto.OpRead,
			PartitionID: fakePartitionID,
			ExtentID:    fakeExtentId,
			Size:        uint32(ep.StripeUnitSize),
			StartT:      time.Now().UnixNano(),
		},
	}

	conn, err := net.Dial("tcp", ec.Hosts[0])
	if err != nil {
		t.Fatal(err)
	}

	defer conn.Close()
	ec.handleReadPacket(p, conn.(*net.TCPConn))
	if p.ResultCode != proto.OpOk {
		t.Fatalf("handleReadPacket fail, error msg:%v", p.GetResultMsg())
	}

	if p.Size != 0 && p.CRC != beforeCrc {
		t.Fatalf("handleReadPacket crc is not same")
	}
}

func TestEcNode_handleStreamReadPacket(t *testing.T) {
	ec := newFakeEcNode(t, fakeStreamReadDataHandler)
	defer os.RemoveAll(ec.path)
	ep := ec.fakeCreateECPartition(t, fakePartitionID)
	ec.fakeCreateExtent(ep, t)

	_ = ec.prepareTestData(t, ep)
	// read
	p := &repl.Packet{
		Object: ep,
		Packet: proto.Packet{
			Magic:       proto.ProtoMagic,
			ReqID:       proto.GenerateRequestID(),
			Opcode:      proto.OpStreamRead,
			PartitionID: fakePartitionID,
			ExtentID:    fakeExtentId,
			Size:        512,
			StartT:      time.Now().UnixNano(),
		},
	}

	conn, err := net.Dial("tcp", ec.Hosts[0])
	if err != nil {
		t.Fatal(err)
	}

	defer conn.Close()
	ec.handleStreamReadPacket(p, conn.(*net.TCPConn))
	if p.ResultCode != proto.OpOk {
		t.Fatalf("handleReadPacket fail, error msg:%v", p.GetResultMsg())
	}
}

func fakeWriteExtent(ep *EcPartition, ecNode *fakeEcNode, t *testing.T) {
	size := int(ep.StripeUnitSize)
	data := make([]byte, size)
	for i := 0; i < size; i++ {
		data[i] = 0
	}

	p := &repl.Packet{
		Object: ep,
		Packet: proto.Packet{
			Magic:       proto.ProtoMagic,
			ReqID:       proto.GenerateRequestID(),
			Opcode:      proto.OpWrite,
			PartitionID: fakePartitionID,
			ExtentID:    fakeExtentId,
			Size:        uint32(size),
			Data:        data,
			StartT:      time.Now().UnixNano(),
		},
	}

	ecNode.handleWritePacket(p)

	if p.ResultCode != proto.OpOk {
		t.Fatalf("handleWritePacket fail, error msg:%v", p.GetResultMsg())
	}
}
