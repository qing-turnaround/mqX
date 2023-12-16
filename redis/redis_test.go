package redis

import (
	"context"
	"testing"
)

const (
	address  = "192.168.5.52:6379"
	password = "123456"
)

// XADD stream1 * key1 key1
func TestXADD(t *testing.T) {
	client := NewClient(address, password, WithDB(6))
	ctx := context.Background()
	res, err := client.XADD(ctx, "stream1", 0, "mssssgg")
	if err != nil {
		t.Error(err)
		return
	}
	t.Log(res)
}

// XREADGROUP GROUP group consumer1 STREAMS stream1 >
func TestXReadGroup(t *testing.T) {
	client := NewClient(address, password, WithDB(6))

	ctx := context.Background()
	res, err := client.XReadGroupPending(ctx, "group1", "consumer1", "stream1")
	if err != nil {
		t.Error(err)
		return
	}
	t.Log(res)
}

// XREAD STREAMS my_streams_topic 0-0
func TestXRead(t *testing.T) {
	client := NewClient(address, password, WithDB(6))
	ctx := context.Background()
	res, err := client.XRead(ctx, "stream1", 10)
	if err != nil {
		t.Error(err)
		return
	}
	t.Log(res)
}
