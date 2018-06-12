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
func (ch Channel) NewExchange(exchange Exchange) error {
	return ch.ExchangeDeclare(
		exchange.Name,
		exchange.ExchangeType,
		exchange.Durable,
		exchange.AutoDeleted,
		exchange.Internal,
		exchange.NoWait,
		nil,
	)
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
