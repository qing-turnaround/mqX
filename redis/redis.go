package redis

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/redis/go-redis/v9"
)

type MsgEntity struct {
	MsgID string
	Val   string
}

const (
	key = "msg"
)

var (
	ErrNoMsg      = errors.New("no msg received")
	ErrXReadGroup = errors.New("redis XREADGROUP groupID/consumerID/topic can't be empty")
	ErrXACK       = errors.New("redis XACK topic | group_id | msg_ id can't be empty")
)

// Client Redis 客户端.
type Client struct {
	opts   *ClientOptions
	client *redis.Client
}

func NewClient(address, password string, opts ...ClientOption) *Client {
	c := Client{
		opts: &ClientOptions{
			address:  address,
			password: password,
		},
	}

	for _, opt := range opts {
		opt(c.opts)
	}

	repairClient(c.opts)

	client := redis.NewClient(&redis.Options{
		Addr:            c.opts.address,
		Password:        c.opts.password,
		DB:              c.opts.db,
		MaxActiveConns:  c.opts.maxActiveConns,
		MaxIdleConns:    c.opts.maxIdleConns,
		ConnMaxIdleTime: time.Duration(c.opts.idleTimeoutSeconds) * time.Second,
	})

	c.client = client
	return &c
}

func (c *Client) Close() error {
	return c.client.Close()
}

func (c *Client) XADD(ctx context.Context, topic string, maxLen int, msg string) (string, error) {
	conn := c.client.Conn()
	defer conn.Close()
	return conn.XAdd(ctx, &redis.XAddArgs{
		Stream: topic,
		MaxLen: int64(maxLen),
		Values: map[string]interface{}{
			key: msg,
		},
	}).Result()
}

func (c *Client) XACK(ctx context.Context, topic, groupID, msgID string) error {
	if topic == "" || groupID == "" || msgID == "" {
		return ErrXACK
	}

	conn := c.client.Conn()
	defer conn.Close()

	reply, err := conn.XAck(ctx, topic, groupID, msgID).Result()
	if err != nil {
		return err
	}
	if reply != 1 {
		return fmt.Errorf("invalid reply: %d", reply)
	}

	return nil
}

func (c *Client) XReadGroupPending(ctx context.Context, groupID, consumerID, topic string) ([]*MsgEntity, error) {
	return c.xReadGroup(ctx, groupID, consumerID, topic, 0, true)
}

func (c *Client) XReadGroup(ctx context.Context, groupID, consumerID, topic string, timeoutMiliSeconds int) ([]*MsgEntity, error) {
	return c.xReadGroup(ctx, groupID, consumerID, topic, timeoutMiliSeconds, false)
}

func (c *Client) xReadGroup(ctx context.Context, groupID, consumerID, topic string, timeoutMiliSeconds int, pending bool) ([]*MsgEntity, error) {
	if groupID == "" || consumerID == "" || topic == "" {
		return nil, ErrXReadGroup
	}
	conn := c.client.Conn()
	defer conn.Close()
	// consumer 刚启动时，批量获取一次分配给本节点，但是还没 ack 的消息进行处理
	// consumer 处理消息之后，如果想给一个坏的 ack，那则是再获取一次 pending 重新走一次流程
	// 分配给本节点，但是尚未 ack 的消息 0-0
	// 拿到尚未分配过的新消息 >
	var reply []redis.XStream
	var err error
	if pending {
		// 倘若 pending 为 true，拉取已经投递却未被ACK的消息，保证消息至少被成功消费1次 此时采用非阻塞模式进行处理
		reply, err = conn.XReadGroup(ctx, &redis.XReadGroupArgs{
			Group:    groupID,
			Consumer: consumerID,
			Streams:  []string{topic, "0"},
		}).Result()
	} else {
		// 倘若 pending 为 false，代表需要消费的是尚未分配给任何 consumer 的新消息，此时会才用阻塞模式执行操作
		reply, err = conn.XReadGroup(ctx, &redis.XReadGroupArgs{
			Group:    groupID,
			Consumer: consumerID,
			Streams:  []string{topic, ">"},
			Block:    time.Duration(timeoutMiliSeconds) * time.Millisecond,
		}).Result()
	}
	if err != nil {
		return nil, err
	}

	if len(reply) == 0 {
		return nil, ErrNoMsg
	}

	var msgSlice []*MsgEntity
	for _, msg := range reply[0].Messages {
		msgSlice = append(msgSlice, &MsgEntity{
			MsgID: msg.ID,
			Val:   msg.Values[key].(string),
		})
	}

	return msgSlice, nil
}

func (c *Client) XRead(ctx context.Context, topic string, count int) ([]*MsgEntity, error) {
	conn := c.client.Conn()
	defer conn.Close()

	reply, err := conn.XRead(ctx, &redis.XReadArgs{
		Streams: []string{topic, "0"},
		Count:   int64(count),
	}).Result()

	if err != nil {
		return nil, err
	}

	if len(reply) == 0 {
		return nil, ErrNoMsg
	}

	var msgSlice []*MsgEntity
	for _, msg := range reply[0].Messages {
		for k, v := range msg.Values {
			fmt.Println(k, v)
		}
		msgSlice = append(msgSlice, &MsgEntity{
			MsgID: msg.ID,
			Val:   msg.Values[key].(string),
		})
	}

	return msgSlice, nil
}
