package amqp

import (
	"encoding/json"

	config "github.com/dynamicgo/go-config"
	"github.com/dynamicgo/mq"
	"github.com/streadway/amqp"
)

type consumerImpl struct {
	conn    *amqp.Connection
	channel *amqp.Channel
	queue   amqp.Queue
	topic   string
	recvq   chan mq.Record
	errorsq chan error
}

func newConsumer(config config.Config) (mq.Consumer, error) {

	cached := config.Get("cached").Int(10)

	consumer := &consumerImpl{
		recvq:   make(chan mq.Record, cached),
		errorsq: make(chan error, cached),
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

	channel.Qos(config.Get("prefetch").Int(1), 0, true)

	consumer.conn = conn
	consumer.channel = channel
	consumer.topic = config.Get("topic").String("test")

	err = channel.ExchangeDeclare(
		consumer.topic,
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

	queue, err := channel.QueueDeclare(
		config.Get("queue").String("test"), // name
		true,  // durable
		false, // delete when unused
		false, // exclusive
		false, // no-wait
		nil,   // arguments
	)

	if err != nil {
		return nil, err
	}

	consumer.queue = queue

	err = channel.QueueBind(
		queue.Name,                          // queue name
		config.Get("routingkey").String(""), // routing key
		consumer.topic,                      // exchange
		false,
		nil)

	if err != nil {
		return nil, err
	}

	deliver, err := channel.Consume(
		queue.Name,
		config.Get("consumer").String(""),
		false,
		false,
		false,
		false,
		nil,
	)

	if err != nil {
		return nil, err
	}

	go consumer.runLoop(deliver)

	return consumer, nil
}

func (consumer *consumerImpl) runLoop(delivery <-chan amqp.Delivery) {
	for d := range delivery {
		var record recordImpl
		err := json.Unmarshal(d.Body, &record)

		if err != nil {
			consumer.errorsq <- err
			d.Ack(false)
			continue
		}

		record.Delivery = &d

		consumer.recvq <- &record
	}
}
func (consumer *consumerImpl) Recv() <-chan mq.Record {
	return consumer.recvq
}

func (consumer *consumerImpl) Errors() <-chan error {
	return consumer.errorsq
}

func (consumer *consumerImpl) Commit(record mq.Record) error {
	r := record.(*recordImpl)

	return r.Delivery.Ack(false)
}
