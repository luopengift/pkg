package kafka2

import (
	"context"
	"fmt"
	"strings"

	"github.com/luopengift/types"

	"github.com/confluentinc/confluent-kafka-go/kafka"
	"github.com/luopengift/log"
)

const (
	// VERSION version
	VERSION = "0.0.1"
)

// Consumer xx
// 一些使用说明:
// sarame.OffsetNewest int64 = -1
// sarame.OffsetOldest int64 = -2
type Consumer struct {
	Addrs           []string    `json:"addrs" yaml:"addrs"` //如果定义了group,则addrs是zookeeper的地址(2181)，否则的话是kafka的地址(9092)
	Topics          []string    `json:"topics" yaml:"topics"`
	Group           string      `json:"group" yaml:"group"`
	Offset          string      `json:"offset" yaml:"offset"`
	Message         chan []byte `json:"-" yaml:"-"` //从这个管道中读取数据
	*kafka.Consumer `json:"-" yaml:"-"`
}

// NewConsumer kafka input
func NewConsumer() *Consumer {
	return new(Consumer)
}

func (c *Consumer) Init(v interface{}) error {
	return types.Format(v, c)
}

func (c *Consumer) LoadConfig(f string) error {
	return types.ParseConfigFile(c, f)
}

func (c *Consumer) Read(p []byte) (cnt int, err error) {
	msg := <-c.Message
	n := copy(p, msg)
	return n, nil
}

func (c *Consumer) readFromTopics(ctx context.Context) error {
LOOP:
	for {
		select {
		case <-ctx.Done():
			break LOOP
		default:
			msg, err := c.Consumer.ReadMessage(-1)
			if err != nil {
				fmt.Printf("Consumer error: %v (%v)\n", err, msg)
				break LOOP
			}
			c.Message <- msg.Value
			//fmt.Printf("Message on %s: %s\n", msg.TopicPartition, string(msg.Value)[:60])
		}
	}
	log.Warn("close client!")
	close(c.Message)
	return c.Consumer.Close()
}

func (c *Consumer) Start() error {
	var err error
	c.Message = make(chan []byte, 1000)

	//c.closechan = make(chan string, 1)
	//c.mux = new(sync.Mutex)
	if c.Consumer, err = kafka.NewConsumer(&kafka.ConfigMap{
		"bootstrap.servers": strings.Join(c.Addrs, ","),
		"group.id":          c.Group,
		"auto.offset.reset": c.Offset,
	}); err != nil {
		return err
	}
	c.Consumer.SubscribeTopics(c.Topics, nil)
	go c.readFromTopics(context.TODO())
	return nil
}

func (c *Consumer) Close() error {
	return c.Close()
}

func (c *Consumer) Version() string {
	return VERSION
}
