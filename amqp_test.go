package amqp

import (
	"sync"
	"testing"

	"github.com/stretchr/testify/require"

	config "github.com/dynamicgo/go-config"
	"github.com/dynamicgo/go-config/source/memory"
)

var configdata = `{

}`

var conf config.Config

func init() {
	conf = config.NewConfig()

	err := conf.Load(memory.NewSource(
		memory.WithData([]byte(configdata)),
	))

	if err != nil {
		panic(err)
	}
}

func TestProduce(t *testing.T) {
	producer, err := newProducer(conf)

	require.NoError(t, err)

	for i := 0; i < 10; i++ {
		record, err := producer.Record([]byte("test"), []byte("testxxx"))

		require.NoError(t, err)

		err = producer.Send(record)

		require.NoError(t, err)
	}
}

func TestConsumer(t *testing.T) {

	var wg sync.WaitGroup

	for i := 0; i < 10; i++ {
		consumer, err := newConsumer(conf)

		require.NoError(t, err)

		c := i

		wg.Add(1)

		go func() {

			println("run consumer", c)

			r := <-consumer.Recv()

			require.NoError(t, consumer.Commit(r))

			println("test", c)

			wg.Done()
		}()

	}

	wg.Wait()
}
