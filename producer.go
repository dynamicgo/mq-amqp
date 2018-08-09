package amqp

import (
	"encoding/json"

	"github.com/dynamicgo/slf4go"

	config "github.com/dynamicgo/go-config"
	"github.com/dynamicgo/mq"
	"github.com/streadway/amqp"
)

type producerImpl struct {
	slf4go.Logger
	conn       *amqp.Connection
	channel    *amqp.Channel
	topic      string
	routingkey string
}

func newProducer(config config.Config) (mq.Producer, error) {
	producer := &producerImpl{
		routingkey: config.Get("routingkey").String(""),
		Logger:     slf4go.Get("amqp"),
	}

	url := config.Get("url").String("amqp://test:test@localhost:5672/")

	conn, err := amqp.Dial(url)

	if err != nil {
		return nil, err
	}

	channel, err := conn.Channel()

	if err != nil {
		return nil, err
	}

	producer.conn = conn
	producer.channel = channel
	producer.topic = config.Get("topic").String("test")

	err = channel.ExchangeDeclare(
		producer.topic,
		"topic",
		true,  // durable
		false, // auto-deleted
		false, // internal
		false, // no-wait
		nil,   // arguments
	)

	if err != nil {
		return nil, err
	}

	c := make(chan amqp.Return, 10)

	channel.NotifyReturn(c)

	producer.DebugF("start trace")

	go func() {
		for r := range c {
			producer.ErrorF("deliver message %s err %s", r.MessageId, r.ReplyText)
		}
	}()

	return producer, nil
}

func (producer *producerImpl) Record(key []byte, value []byte) (mq.Record, error) {
	return &recordImpl{
		Jkey:   key,
		Jvalue: value,
	}, nil
}
func (producer *producerImpl) Send(record mq.Record) error {

	data, err := json.Marshal(record)

	if err != nil {
		return err
	}

	return producer.channel.Publish(
		producer.topic,
		"",
		true,
		false,
		amqp.Publishing{
			ContentType:     "application/json",
			ContentEncoding: "string",
			Body:            data,
			DeliveryMode:    amqp.Persistent,
		},
	)
}
func (producer *producerImpl) Batch(records []mq.Record) error {
	for _, record := range records {
		if err := producer.Send(record); err != nil {
			return err
		}
	}

	return nil
}
