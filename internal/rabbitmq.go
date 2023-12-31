package internal

import (
	"context"
	"fmt"
	amqp "github.com/rabbitmq/amqp091-go"
	"log"
)

type RabbitMQClient struct {
	conn *amqp.Connection
	ch   *amqp.Channel
}

func ConnectRabbitMQ(username, password, host, vhost string) (*amqp.Connection, error) {
	conn, err := amqp.Dial(fmt.Sprintf("amqp://%s:%s@%s/%s", username, password, host, vhost))
	if err != nil {
		return conn, err
	}

	return conn, nil
}

func NewRabbitMQClient(conn *amqp.Connection) (RabbitMQClient, error) {
	ch, err := conn.Channel()
	if err != nil {
		return RabbitMQClient{}, err
	}

	// Puts the Channel in confirm mode, which will allow waiting for ACK or NACK from the receiver
	if err := ch.Confirm(false); err != nil {
		return RabbitMQClient{}, err
	}

	return RabbitMQClient{
		conn: conn,
		ch:   ch,
	}, nil
}

func (rc RabbitMQClient) Close() error {
	return rc.ch.Close()
}

func (rc RabbitMQClient) CreateQueue(name string, durable, autoDelete bool) error {
	_, err := rc.ch.QueueDeclare(name, durable, autoDelete, false, false, nil)

	return err
}

// CreateBinding is used to connect a queue to an Exchange using the binding rule
func (rc RabbitMQClient) CreateBinding(name, binding, exchange string) error {
	// leaveing nowait false, having nowait set to false wctxill cause the channel to return an error and close if it cannot bind
	// the final argument is the extra headers
	return rc.ch.QueueBind(name, binding, exchange, false, nil)
}

// Send is used to publish a payload onto an exchange with a given routingkey
func (rc RabbitMQClient) Send(ctx context.Context, exchange, routingKey string, options amqp.Publishing) error {
	return rc.ch.PublishWithContext(ctx,
		exchange,   // exchange
		routingKey, // routing key
		// Mandatory is used when we HAVE to have the message return an error, if there is no route or queue then
		// setting this to true will make the message bounce back
		// If this is False, and the message fails to deliver, it will be dropped
		true, // mandatory
		// immediate Removed in MQ 3 or up https://blog.rabbitmq.com/posts/2012/11/breaking-things-with-rabbitmq-3-0§
		false,   // immediate
		options, // amqp publishing struct
	)
}

// Consume is a wrapper around consume, it will return a Channel that can be used to digest messages
// Queue is the name of the queue to Consume
// Consumer is a unique identifier for the service instance that is consuming, can be used to cancel etc
// autoAck is important to understand, if set to true, it will automatically Acknowledge that processing is done
// This is good, but remember that if the Process fails before completion, then an ACK is already sent, making a message lost
// if not handled properly
func (rc RabbitMQClient) Consume(queue, consumer string, autoAck bool) (<-chan amqp.Delivery, error) {
	return rc.ch.Consume(queue, consumer, autoAck, false, false, false, nil)
}

// SendWithConfirm is used to publish a payload onto an exchange with a given routingkey
func (rc RabbitMQClient) SendWithConfirm(ctx context.Context, exchange, routingKey string, options amqp.Publishing) error {
	// PublishWithDeferredConfirmWithContext will wait for server to ACK the message
	confirmation, err := rc.ch.PublishWithDeferredConfirmWithContext(ctx,
		exchange,   // exchange
		routingKey, // routing key
		// Mandatory is used when we HAVE to have the message return an error, if there is no route or queue then
		// setting this to true will make the message bounce back
		// If this is False, and the message fails to deliver, it will be dropped
		true, // mandatory
		// immediate Removed in MQ 3 or up https://blog.rabbitmq.com/posts/2012/11/breaking-things-with-rabbitmq-3-0§
		false,   // immediate
		options, // amqp publishing struct
	)
	if err != nil {
		return err
	}
	// Blocks until ACK from Server is receieved
	log.Println(confirmation.Wait())
	return nil
}
