package ecnode

import (
	"encoding/json"
	"fmt"
	"github.com/chubaofs/chubaofs/proto"
	"github.com/chubaofs/chubaofs/repl"
	"github.com/chubaofs/chubaofs/util"
	"github.com/chubaofs/chubaofs/util/config"
	"hash/crc32"
	"net"
	"os"
	"sync"
	"testing"
	"time"
)

const (
	fakePartitionID = 1
	//testBasePath    = "/tmp/cfs"
	testBasePath = "/Users/liuchengyu/cfs"
	testDiskPath = testBasePath + "/disk"
	fakeExtentId = 1025
)

type fakeHandler func(e *fakeEcNode, p *repl.Packet, conn net.Conn)

type fakeEcNode struct {
	EcNode
	Hosts []string
	path  string
}

func TestEcNode_handlePacketToCreateExtent(t *testing.T) {
	ec := newFakeEcNode(t, fakeCreateExtentPacketHandle)
	defer os.RemoveAll(ec.path)
	ep := ec.fakeCreateECPartition(t, fakePartitionID)

	p := &repl.Packet{
		Object: ep,
		Packet: proto.Packet{
			Magic:       proto.ProtoMagic,
			ReqID:       proto.GenerateRequestID(),
			Opcode:      proto.OpCreateExtent,
			PartitionID: fakePartitionID,
			StartT:      time.Now().UnixNano(),
		},
	}

	ec.handlePacketToCreateExtent(p)
	if p.ResultCode != proto.OpOk {
		t.Fatalf("handlePacketToCreateExtent fail, error msg:%v", p.GetResultMsg())
	}

	if p.ExtentID == 0 {
		t.Fatal("handlePacketToCreateExtent fail")
	}
}

func TestEcNode_createExtentOnFollower(t *testing.T) {
	e := newFakeEcNode(t, fakeCreateExtentPacketHandle)
	defer os.RemoveAll(e.path)
	ep := e.fakeCreateECPartition(t, fakePartitionID)
	ok := e.createExtentOnFollower(ep, fakeExtentId)
	if !ok {
		t.Errorf("createExtentOnFollower() = %v, want true", ok)
	}
}

func newFakeEcNode(t *testing.T, handler fakeHandler) *fakeEcNode {
	path := fmt.Sprintf("%s%d", testDiskPath, time.Now().Nanosecond())
	_ = os.MkdirAll(path, 0766)

	e := &fakeEcNode{
		EcNode: EcNode{
			clusterID:       "ecnode-cluster",
			port:            "17310",
			cellName:        "cell-01",
			localIP:         "127.0.0.1",
			localServerAddr: "127.0.0.1:17310",
			nodeID:          uint64(123),
			stopC:           make(chan bool),
		},
		Hosts: []string{
			"127.0.0.1:17310",
			"127.0.0.1:17311",
			"127.0.0.1:17312",
			"127.0.0.1:17313",
			"127.0.0.1:17314",
			"127.0.0.1:17315",
		},
		path: path,
	}

	wg := sync.WaitGroup{}
	for _, host := range e.Hosts {
		wg.Add(1)
		go e.startFakeServiceForTest(t, host, &wg, handler)
	}
	wg.Wait()

	err := e.startSpaceManager(&config.Config{
		Data: map[string]interface{}{
			ConfigKeyDisks: []interface{}{
				fmt.Sprintf("%s:123040", path),
			},
		},
	})

	if err != nil {
		t.Errorf("startSpaceManager error = %v", err)
	}

	return e
}

func (e *fakeEcNode) fakeCreateExtent(ep *EcPartition, t *testing.T) {
	p := &repl.Packet{
		Packet: proto.Packet{
			Magic:       proto.ProtoMagic,
			ReqID:       proto.GenerateRequestID(),
			Opcode:      proto.OpCreateExtent,
			PartitionID: ep.PartitionID,
			StartT:      time.Now().UnixNano(),
			ExtentID:    fakeExtentId,
		},
	}

	err := e.Prepare(p)
	if err != nil {
		t.Errorf("Prepare() error = %v", err)
	}

	conn, err := net.Dial("tcp", e.Hosts[0])
	if err != nil {
		t.Fatal(err)
	}

	defer conn.Close()
	err = e.OperatePacket(p, conn.(*net.TCPConn))
	if err != nil {
		t.Errorf("OperatePacket() error = %v", err)
	}

	err = e.Post(p)
	if err != nil {
		t.Errorf("Post() error = %v", err)
	}

	if p.ResultCode != proto.OpOk {
		t.Fatalf("fakeCreateExtent fail, error msg:%v", p.GetResultMsg())
	}

	if p.ExtentID != fakeExtentId {
		t.Fatal("fakeCreateExtent fail, error not set ExtentId")
	}
}

func (e *fakeEcNode) fakeCreateECPartition(t *testing.T, partitionId uint64) (ep *EcPartition) {
	req := &proto.CreateEcPartitionRequest{
		PartitionID:    partitionId,
		PartitionSize:  uint64(5 * util.GB), // 5GB
		VolumeID:       "ltptest",
		StripeUnitSize: 4 * util.KB,  // 4KB
		ExtentFileSize: 64 * util.MB, // 64 KB
		DataNodeNum:    uint32(4),
		ParityNodeNum:  uint32(2),
		Hosts:          e.Hosts,
	}

	task := proto.NewAdminTask(proto.OpCreateEcDataPartition, e.Hosts[0], req)
	body, err := json.Marshal(task)
	p := &repl.Packet{
		Packet: proto.Packet{
			Magic:       proto.ProtoMagic,
			ReqID:       proto.GenerateRequestID(),
			Opcode:      proto.OpCreateEcDataPartition,
			PartitionID: partitionId,
			Data:        body,
			Size:        uint32(len(body)),
			StartT:      time.Now().UnixNano(),
		},
	}

	err = e.Prepare(p)
	if err != nil {
		t.Errorf("Prepare() error = %v", err)
	}

	conn, err := net.Dial("tcp", e.Hosts[0])
	if err != nil {
		t.Fatal(err)
	}

	defer conn.Close()
	err = e.OperatePacket(p, conn.(*net.TCPConn))
	if err != nil {
		t.Errorf("OperatePacket() error = %v", err)
	}

	err = e.Post(p)
	if err != nil {
		t.Errorf("Post() error = %v", err)
	}

	return e.space.Partition(partitionId)
}

func (e *fakeEcNode) startFakeServiceForTest(t *testing.T, host string, wg *sync.WaitGroup, handler fakeHandler) {
	fmt.Println(fmt.Sprintf("host:%v listening", host))
	l, err := net.Listen("tcp", host)
	if err != nil {
		t.Fatal(err)
	}
	defer l.Close()
	wg.Done()

	for {
		conn, err := l.Accept()
		if err != nil {
			continue
		}

		fmt.Printf("recive conn[local:%v remote:%v]\n", conn.LocalAddr(), conn.RemoteAddr())
		go e.fakeServiceHandler(handler, conn, t)
	}
}

func (e *fakeEcNode) fakeServiceHandler(handler fakeHandler, conn net.Conn, t *testing.T) {
	defer conn.Close()
	request := repl.NewPacket()
	if err := request.ReadFromConnFromCli(conn, proto.NoReadDeadlineTime); err != nil {
		//fmt.Println(err)
		return
	}

	if handler != nil {
		handler(e, request, conn)
		if err := request.WriteToConn(conn); err != nil {
			t.Fatal(err)
		}
	}
}

func (e *fakeEcNode) prepareTestData(t *testing.T, ep *EcPartition) uint32 {
	size := int(ep.StripeUnitSize)
	data := make([]byte, size)
	for i := 0; i < size; i++ {
		data[i] = 0
	}

	crc := crc32.ChecksumIEEE(data)
	p := &repl.Packet{
		Object: ep,
		Packet: proto.Packet{
			Magic:       proto.ProtoMagic,
			ReqID:       proto.GenerateRequestID(),
			Opcode:      proto.OpWrite,
			PartitionID: ep.PartitionID,
			ExtentID:    fakeExtentId,
			Size:        uint32(size),
			CRC:         crc,
			Data:        data,
			StartT:      time.Now().UnixNano(),
		},
	}

	e.handleWritePacket(p)

	if p.ResultCode != proto.OpOk {
		t.Fatalf("prepareTestData fail, error msg:%v", p.GetResultMsg())
	}

	return crc
}

func fakeCreateExtentPacketHandle(e *fakeEcNode, request *repl.Packet, conn net.Conn) {
	request.ExtentID = fakeExtentId
	request.ResultCode = proto.OpOk
}

func fakeStreamReadDataHandler(e *fakeEcNode, request *repl.Packet, conn net.Conn) {
	request.Data = make([]byte, request.Size)
	for i := 0; i < int(request.Size); i++ {
		request.Data[i] = 0
	}

	request.CRC = crc32.ChecksumIEEE(request.Data)
	request.ResultCode = proto.OpOk
}
