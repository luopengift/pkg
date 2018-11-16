package kafka

import (
	"github.com/Shopify/sarama"
	"github.com/luopengift/golibs/channel"
	"github.com/luopengift/log"
	"github.com/luopengift/types"
)

type Producer struct {
	Addrs    []string         `json:"addrs" yaml:"addrs"`
	Topic    string           `json:"topic" yaml:"topic"`
	MaxProcs int              `json:"max_procs" yaml:"max_procs"` //最大并发写协程, 由于并发写入topic,写入顺序不可控,想要严格数序的话,maxThreads = 1即可
	Message  chan []byte      `json:"-" yaml:"-"`                 //将数据写入这个管道中
	channel  *channel.Channel //并发写topic的协程控制
}

func NewProducer() *Producer {
	return new(Producer)
}
func (p *Producer) Init(v interface{}) error {
	return types.Format(v, p)
}

func (p *Producer) LoadConfig(f string) error {
	return types.ParseConfigFile(p, f)
}

func (p *Producer) ChanInfo() string {
	return p.channel.String()
}

func (p *Producer) Write(msg []byte) (int, error) {
	p.Message <- msg
	return len(msg), nil
}

func (p *Producer) Close() error {
	return p.Close()
}

func (p *Producer) Start() error {
	p.Message = make(chan []byte, p.MaxProcs)
	p.channel = channel.NewChannel(p.MaxProcs)
	go p.WriteToTopic()
	return nil
}

func (p *Producer) WriteToTopic() error {

	config := sarama.NewConfig()
	config.ClientID = "TransportProducer"
	config.Producer.Return.Successes = true
	if err := config.Validate(); err != nil {
		log.Error("<config error> %v", err)
		return err
	}

	producer, err := sarama.NewSyncProducer(p.Addrs, config)
	if err != nil {
		log.Error("<Failed to produce message> %v", err)
		return err
	}
	defer producer.Close()

	for {
		select {
		case message := <-p.Message:
			p.channel.Add()
			go func(message []byte) {
				msg := &sarama.ProducerMessage{
					Topic: p.Topic,
					//Partition: int32(-1),
					//Key:       sarama.StringEncoder("key"),
					Value: sarama.ByteEncoder(message),
				}
				if partition, offset, err := producer.SendMessage(msg); err != nil {
					log.Error("<write to kafka error,partition=%v,offset=%v> %v", partition, offset, err)
				}
				p.channel.Done()
			}(message)
		}
	}
}

func (p *Producer) Version() string {
	return "VERSION"
}
