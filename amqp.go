package amqp

import (
	config "github.com/dynamicgo/go-config"
	"github.com/dynamicgo/mq"
)

func init() {
	// register driver
	mq.OpenDriver("amqp", func(config config.Config) (mq.Consumer, error) {

		return newConsumer(config)

	}, func(config config.Config) (mq.Producer, error) {

		return newProducer(config)

	})
}
