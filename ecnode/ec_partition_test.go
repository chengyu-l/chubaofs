package ecnode

import (
	"encoding/json"
	"github.com/chubaofs/chubaofs/proto"
	"github.com/chubaofs/chubaofs/repl"
	"github.com/chubaofs/chubaofs/storage"
	"hash/crc32"
	"net"
	"os"
	"testing"
	"time"
)

func Test_getChangeEcPartitionMemberRequest(t *testing.T) {
	request := &proto.ChangeEcPartitionMembersRequest{}
	request.PartitionId = fakePartitionID
	request.Hosts = []string{
		"127.0.0.1:17310",
		"127.0.0.1:17311",
		"127.0.0.1:17312",
		"127.0.0.1:17313",
		"127.0.0.1:17314",
		"127.0.0.1:17315",
	}

	task := proto.AdminTask{}
	task.OpCode = proto.OpChangeEcPartitionMembers
	task.PartitionID = fakePartitionID
	task.CreateTime = time.Now().Unix()
	task.Request = request

	marshal, err := json.Marshal(task)
	if err != nil {
		t.Fatal("marshal ChangeEcPartitionMembersRequest fail")
	}

	memberRequest, err := getChangeEcPartitionMemberRequest(marshal)
	if err != nil {
		t.Fatal("unmarshal ChangeEcPartitionMembersRequest fail")
	}

	if len(memberRequest.Hosts) != 6 {
		t.Fatal("unmarshal ChangeEcPartitionMembersRequest host fail")
	}
}

func TestEcNode_handleUpdateEcDataPartition(t *testing.T) {
	// clean data
	defer os.RemoveAll(testBasePath)

	ec := newFakeEcNode(t, nil)
	defer os.RemoveAll(ec.path)
	ep := ec.fakeCreateECPartition(t, fakePartitionID)

	request := &proto.ChangeEcPartitionMembersRequest{
		PartitionId: fakePartitionID,
		Hosts: []string{
			"127.0.0.1:17310",
			"127.0.0.1:17311",
			"127.0.0.1:17312",
			"127.0.0.1:17313",
			"127.0.0.1:17314",
			"127.0.0.1:17316",
		},
	}

	task := proto.NewAdminTask(proto.OpUpdateEcDataPartition, "", request)
	data, err := json.Marshal(task)
	if err != nil {
		return
	}

	err = ep.updatePartitionLocal(data)
	if err != nil {
		t.Fatal("handleListExtents fail")
	}

	if ep.Hosts[len(ep.Hosts)-1] != "127.0.0.1:17316" {
		t.Fatal("handleListExtents fail, host not same")
	}
}

func fakeUpdatePartitionHandle(e *fakeEcNode, request *repl.Packet, conn net.Conn) {
	request.ExtentID = fakeExtentId
	request.ResultCode = proto.OpOk
}

func TestEcPartition_updatePartitionAllEcNode(t *testing.T) {
	ec := newFakeEcNode(t, fakeUpdatePartitionHandle)
	defer os.RemoveAll(ec.path)
	ep := ec.fakeCreateECPartition(t, fakePartitionID)
	ec.fakeCreateExtent(ep, t)

	request := &proto.ChangeEcPartitionMembersRequest{
		PartitionId: fakePartitionID,
		Hosts: []string{
			"127.0.0.1:17310",
			"127.0.0.1:17311",
			"127.0.0.1:17312",
			"127.0.0.1:17313",
			"127.0.0.1:17314",
			"127.0.0.1:17315",
		},
	}

	task := proto.NewAdminTask(proto.OpUpdateEcDataPartition, "", request)
	data, err := json.Marshal(task)
	if err != nil {
		return
	}

	err = ep.updatePartitionAllEcNode(&ec.EcNode, request, data)
	if err != nil {
		t.Fatal("updatePartitionAllEcNode fail")
	}
}

func Test_listExtentsFollower(t *testing.T) {
	ec := newFakeEcNode(t, fakeListExtentsHandle)
	defer os.RemoveAll(ec.path)
	ep := ec.fakeCreateECPartition(t, fakePartitionID)

	extents, err := ep.listExtentsFollower("127.0.0.1:17315")
	if err != nil {
		t.Fatalf("listExtentsFollower error:%v\n", err)
	}

	if len(extents) != 1 {
		t.Fatal("listExtentsFollower fail")
	}
}

func TestEcPartition_getExtentListAllEcNode(t *testing.T) {
	ec := newFakeEcNode(t, fakeListExtentsHandle)
	defer os.RemoveAll(ec.path)
	ep := ec.fakeCreateECPartition(t, fakePartitionID)
	ec.fakeCreateExtent(ep, t)
	fakeWriteExtent(ep, ec, t)

	time.Sleep(61 * time.Second)
	request := &proto.ChangeEcPartitionMembersRequest{
		PartitionId: fakePartitionID,
		Hosts: []string{
			"127.0.0.1:17310",
			"127.0.0.1:17311",
			"127.0.0.1:17312",
			"127.0.0.1:17313",
			"127.0.0.1:17314",
			"127.0.0.1:17315",
		},
	}

	maxExtentInfoEcNode, list, err := ep.listExtentsAllEcNode(&ec.EcNode, request)
	if err != nil {
		t.Fatalf("listExtentsAllEcNode error:%v\n", err)
	}

	if maxExtentInfoEcNode == nil || len(list) != int(ep.DataNodeNum+ep.ParityNodeNum) {
		t.Fatal("listExtentsAllEcNode fail")
	}
}

func fakeListExtentsHandle(e *fakeEcNode, request *repl.Packet, conn net.Conn) {
	request.ExtentID = fakeExtentId
	request.ResultCode = proto.OpOk

	list := []*storage.ExtentInfo{
		{
			FileID:     1025,
			Size:       4096,
			Crc:        0,
			IsDeleted:  false,
			ModifyTime: 1597644402,
			Source:     "",
		},
	}

	marshal, _ := json.Marshal(list)
	request.Data = marshal
	request.Size = uint32(len(marshal))
	request.CRC = crc32.ChecksumIEEE(marshal)
}

func TestEcPartition_recoverRead(t *testing.T) {
	ec := newFakeEcNode(t, fakeRecoverReadHandle)
	defer os.RemoveAll(ec.path)
	ep := ec.fakeCreateECPartition(t, fakePartitionID)

	tests := []struct {
		name string
		info *storage.ExtentInfo
	}{
		{
			name: "test-1",
			info: &storage.ExtentInfo{
				FileID:     1025,
				Size:       40960,
				Crc:        0,
				IsDeleted:  false,
				ModifyTime: 1597644402,
				Source:     "",
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if err := ep.repairExtentData(&ec.EcNode, tt.info); err != nil {
				t.Errorf("reconstructStripeData() error = %v", err)
			}
		})
	}
}

func fakeRecoverReadHandle(e *fakeEcNode, request *repl.Packet, conn net.Conn) {
	data := make([]byte, int(request.Size))
	for i := 0; i < int(request.Size); i++ {
		data[i] = 0
	}

	request.ExtentID = fakeExtentId
	request.ResultCode = proto.OpOk
	request.Size = uint32(len(data))
	request.Data = data
	request.CRC = crc32.ChecksumIEEE(data)
}

