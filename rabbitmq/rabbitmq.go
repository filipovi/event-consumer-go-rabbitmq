package rabbitmq

import (
	"github.com/streadway/amqp"
)

// Channel is the RabbitMQ Channel structure
type Channel struct {
	*amqp.Channel
}

// Exchange contains the configuration of a RabbitMQ Exchange
type Exchange struct {
	Name         string
	ExchangeType string
	Durable      bool
	AutoDeleted  bool
	Internal     bool
	NoWait       bool
}

// NewExchange declares a RabbitMQ Exchange
func (ch Channel) NewExchange(name string) error {
	return ch.ExchangeDeclare(
		name,
		"fanout", // type
		true,     // durable
		false,    // auto-deleted
		false,    // internal
		false,    // no-wait
		nil,
	)
}

// NewQueue declares a Rabbitmq Queue
func (ch Channel) NewQueue(name string) (amqp.Queue, error) {
	return ch.QueueDeclare(
		name,  // name
		false, // durable
		false, // delete when unused
		true,  // exclusive
		false, // no-wait
		nil,   // arguments
	)
}

// BindQueue bind the Queue with the Exchange
func (ch Channel) BindQueue(name string, exchangeName string) error {
	return ch.QueueBind(
		name,         // queue name
		"",           // routing key
		exchangeName, // exchange
		false,
		nil)
}

// Send adds a new Message in the Exchange
func (ch Channel) Send(body []byte, name string) error {
	return ch.Publish(
		name,  // exchange name
		"",    // routing key
		false, // mandatory
		false, // immediate
		amqp.Publishing{
			ContentType: "text/plain",
			Body:        body,
		})
}

// New returns a RabbitMQ Connection
func New(url string) (*Channel, error) {
	conn, err := amqp.Dial(url)
	if err != nil {
		return nil, err
	}
	ch, err := conn.Channel()
	if err != nil {
		return nil, err
	}

	return &Channel{ch}, nil
}
