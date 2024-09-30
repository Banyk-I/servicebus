package ServiceBus

import (
	"errors"
	"fmt"
	"github.com/streadway/amqp"
	"time"
)

type RabbitMQClient struct {
	Connection *amqp.Connection
	Channel    *amqp.Channel
	Exchange   string
	Queue      string
	Serializer *JSONSerializer
}

func NewRabbitMQClient(amqpURL, exchange, queue string) (*RabbitMQClient, error) {
	conn, err := amqp.Dial(amqpURL)
	if err != nil {
		return nil, fmt.Errorf("Failed to connect to RabbitMQ: %v", err)
	}

	ch, err := conn.Channel()
	if err != nil {
		conn.Close()
		return nil, fmt.Errorf("Failed to open a channel: %v", err)
	}

	client := &RabbitMQClient{
		Connection: conn,
		Channel:    ch,
		Exchange:   exchange,
		Queue:      queue,
		Serializer: &JSONSerializer{},
	}

	if err := client.createExchange(); err != nil {
		client.closeChanelConnection()
		return nil, err
	}

	if err := client.createQueue(); err != nil {
		client.closeChanelConnection()
		return nil, err
	}

	if err := client.bindQueueToExchange(); err != nil {
		client.closeChanelConnection()
		return nil, err
	}

	// Set QoS
	if err := client.Channel.Qos(1, 0, false); err != nil {
		return nil, fmt.Errorf("Failed to set QoS: %v", err)
	}

	// Enable Publisher Confirms
	if err := client.Channel.Confirm(false); err != nil {
		return nil, fmt.Errorf("Failed to enable publisher confirms: %v", err)
	}

	return client, nil
}

func (client *RabbitMQClient) closeChanelConnection() {
	client.Channel.Close()
	client.Connection.Close()
}

func (client *RabbitMQClient) createExchange() error {
	if client.Exchange != "" {
		return client.Channel.ExchangeDeclare(
			client.Exchange,
			"direct",
			true,
			false,
			false,
			false,
			nil)
	}
	return nil
}

func (client *RabbitMQClient) createQueue() error {
	if client.Queue != "" {
		_, err := client.Channel.QueueDeclare(
			client.Queue,
			true,
			false,
			false,
			false,
			nil)

		return err
	}
	return nil
}

func (client *RabbitMQClient) bindQueueToExchange() error {
	return client.Channel.QueueBind(
		client.Queue,
		"",
		client.Exchange,
		false,
		nil)
}

// Send a message to RabbitMQ
func (client *RabbitMQClient) Send(routingKey string, message Message) error {
	if client.Connection == nil {
		return errors.New("No RabbitMQ connection")
	}

	if client.Channel == nil {
		return errors.New("No RabbitMQ channel")
	}

	body, err := client.Serializer.Marshal(message)
	if err != nil {
		return err
	}

	err = client.Channel.Publish(
		client.Exchange,
		routingKey, // routing key passed as argument
		false,
		false,
		amqp.Publishing{
			ContentType: "application/json",
			Body:        body,
		},
	)

	if err != nil {
		return err
	}

	// Wait for the confirmation
	select {
	case confirm := <-client.Channel.NotifyPublish(make(chan amqp.Confirmation, 1)):
		if !confirm.Ack {
			return errors.New("Message was not acknowledged")
		}
	case <-time.After(5 * time.Second):
		return errors.New("No confirmation received after 5 seconds")
	}

	return nil
}

// Consume messages from RabbitMQ
func (client *RabbitMQClient) Consume() (<-chan amqp.Delivery, error) {
	msgs, err := client.Channel.Consume(
		client.Queue, // queue name
		"",           // consumer tag
		false,        // auto-ack
		false,        // exclusive
		false,        // no-local
		false,        // no-wait
		nil,          // arguments
	)

	if err != nil {
		return nil, fmt.Errorf("Failed to register a consumer: %v", err)
	}

	return msgs, nil
}

// Close the RabbitMQ connection and channel
func (client *RabbitMQClient) Close() error {
	if err := client.Channel.Close(); err != nil {
		return fmt.Errorf("Failed to close channel: %v", err)
	}

	if err := client.Connection.Close(); err != nil {
		return fmt.Errorf("Failed to close connection: %v", err)
	}

	return nil
}
