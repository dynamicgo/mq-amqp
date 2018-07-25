package amqp

import (
	"encoding/json"

	config "github.com/dynamicgo/go-config"
	"github.com/dynamicgo/mq"
	"github.com/streadway/amqp"
)

type producerImpl struct {
	conn       *amqp.Connection
	channel    *amqp.Channel
	topic      string
	routingkey string
}

func newProducer(config config.Config) (mq.Producer, error) {
	producer := &producerImpl{
		routingkey: config.Get("routingkey").String(""),
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
		false,
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
