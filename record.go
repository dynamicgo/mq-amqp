package amqp

import "github.com/streadway/amqp"

type recordImpl struct {
	Jkey     []byte         `json:"key"`
	Jvalue   []byte         `json:"value"`
	Delivery *amqp.Delivery `json:"-"`
}

func (record *recordImpl) Key() []byte {
	return record.Jkey
}

func (record *recordImpl) Value() []byte {
	return record.Jvalue
}
