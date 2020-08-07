package ecnode

import (
	"fmt"
	"testing"
)

func TestNewEcStripe(t *testing.T) {
	fmt.Println(1001 % 6)
	ep := &EcPartition{
		EcPartitionMetaData: EcPartitionMetaData{
			DataNodeNum:   4,
			ParityNodeNum: 2,
			Hosts: []string{
				"1.1.1.1",
				"2.2.2.2",
				"3.3.3.3",
				"4.4.4.4",
				"5.5.5.5",
				"6.6.6.6",
			},
		},
	}

	e := &EcNode{localServerAddr: "1.1.1.1"}

	stripe, _ := NewEcStripe(e, ep, fakeExtentId)
	hosts := stripe.hosts
	if hosts[0] != "6.6.6.6" ||
		hosts[1] != "1.1.1.1" ||
		hosts[2] != "2.2.2.2" ||
		hosts[3] != "3.3.3.3" ||
		hosts[4] != "4.4.4.4" ||
		hosts[5] != "5.5.5.5" {
		t.Fatal("NewExtent calc host fail")
	}
}

func Test_ecStripe_calcNode(t *testing.T) {
	ep := &EcPartition{
		EcPartitionMetaData: EcPartitionMetaData{
			StripeUnitSize: 64,
			DataNodeNum:    4,
			ParityNodeNum:  2,
			Hosts: []string{
				"1.1.1.1",
				"2.2.2.2",
				"3.3.3.3",
				"4.4.4.4",
				"5.5.5.5",
				"6.6.6.6",
			},
		},
	}

	e := &EcNode{}
	ee, _ := NewEcStripe(e, ep, 6)

	tests := []struct {
		name         string
		extentOffset uint64
		want         string
	}{
		{
			name:         "1",
			extentOffset: 10,
			want:         "1.1.1.1",
		},
		{
			name:         "2",
			extentOffset: 150,
			want:         "3.3.3.3",
		},
		{
			name:         "3",
			extentOffset: 400,
			want:         "3.3.3.3",
		},
		{
			name:         "4",
			extentOffset: 500,
			want:         "4.4.4.4",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := ee.calcNode(tt.extentOffset); got != tt.want {
				t.Errorf("calcNode() = %v, want %v", got, tt.want)
			}
		})
	}
}

func Test_ecStripe_calcExtentFileOffset(t *testing.T) {
	ep := &EcPartition{
		EcPartitionMetaData: EcPartitionMetaData{
			StripeUnitSize: 64,
			DataNodeNum:    4,
			ParityNodeNum:  2,
			Hosts: []string{
				"1.1.1.1",
				"2.2.2.2",
				"3.3.3.3",
				"4.4.4.4",
				"5.5.5.5",
				"6.6.6.6",
			},
		},
	}

	e := &EcNode{}
	ee, _ := NewEcStripe(e, ep, 6)

	tests := []struct {
		name         string
		extentOffset uint64
		want         uint64
	}{
		{
			name:         "0",
			extentOffset: 0,
			want:         0,
		},
		{
			name:         "1",
			extentOffset: 10,
			want:         10,
		},
		{
			name:         "2",
			extentOffset: 64,
			want:         0,
		},
		{
			name:         "3",
			extentOffset: 400,
			want:         80,
		},
		{
			name:         "4",
			extentOffset: 257,
			want:         65,
		},
		{
			name:         "5",
			extentOffset: 600,
			want:         152,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := ee.calcExtentFileOffset(tt.extentOffset); got != tt.want {
				t.Errorf("calcNode() = %v, want %v", got, tt.want)
			}
		})
	}
}

func Test_ecStripe_calcCanReadSize(t *testing.T) {
	ep := &EcPartition{
		EcPartitionMetaData: EcPartitionMetaData{
			StripeUnitSize: 64,
			DataNodeNum:    4,
			ParityNodeNum:  2,
			Hosts: []string{
				"1.1.1.1",
				"2.2.2.2",
				"3.3.3.3",
				"4.4.4.4",
				"5.5.5.5",
				"6.6.6.6",
			},
		},
	}

	e := &EcNode{}
	ee, _ := NewEcStripe(e, ep, 6)

	tests := []struct {
		name         string
		canReadSize  uint64
		extentOffset uint64
		want         uint64
	}{
		{
			name:         "1-1",
			canReadSize:  10,
			extentOffset: 10,
			want:         10,
		},
		{
			name:         "1-2",
			canReadSize:  65,
			extentOffset: 10,
			want:         54,
		},
		{
			name:         "2-1",
			canReadSize:  30,
			extentOffset: 300,
			want:         20,
		},
		{
			name:         "2-2",
			canReadSize:  15,
			extentOffset: 300,
			want:         15,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := ee.calcCanReadSize(tt.extentOffset, tt.canReadSize); got != tt.want {
				t.Errorf("calcNode() = %v, want %v", got, tt.want)
			}
		})
	}
}
