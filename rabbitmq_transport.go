package ServiceBus

import (
	"errors"
	"github.com/streadway/amqp"
	"log"
)

type RabbitMQClient struct {
	Connection *amqp.Connection
	Channel    *amqp.Channel
	Exchange   string
	Queue      string
	Serializer *JSONSerializer
}

func NewRabbitMQClient(amqpURL, exchange, queue string) (*RabbitMQClient, error) {
	// Logging the initiation of RabbitMQ connection
	log.Println("Initializing RabbitMQ connection...")

	conn, err := amqp.Dial(amqpURL)
	if err != nil {
		log.Printf("Failed to connect to RabbitMQ: %v\n", err)
		return nil, err
	}

	log.Println("Connection to RabbitMQ established successfully.")

	// Create channel
	ch, err := conn.Channel()
	if err != nil {
		log.Printf("Failed to open a channel: %v\n", err)
		conn.Close()
		return nil, err
	}
	log.Println("Channel opened successfully.")

	// Initialize the serializer
	serializer := &JSONSerializer{}

	client := &RabbitMQClient{
		Connection: conn,
		Channel:    ch,
		Exchange:   exchange,
		Queue:      queue,
		Serializer: serializer,
	}

	// Create exchange
	if err := client.createExchange(); err != nil {
		log.Printf("Failed to create exchange: %v\n", err)
		client.closeChanelConnection()
		return nil, err
	}

	// Create queue
	if err := client.createQueue(); err != nil {
		log.Printf("Failed to create queue: %v\n", err)
		client.closeChanelConnection()
		return nil, err
	}

	// Bind queue to exchange
	if err := client.bindQueueToExchange(); err != nil {
		log.Printf("Failed to bind queue to exchange: %v\n", err)
		client.closeChanelConnection()
		return nil, err
	}

	log.Println("RabbitMQ setup completed successfully.")
	return client, nil
}

func (client *RabbitMQClient) closeChanelConnection() {
	log.Println("Closing RabbitMQ channel and connection...")
	client.Channel.Close()
	client.Connection.Close()
	log.Println("RabbitMQ channel and connection closed.")
}

func (client *RabbitMQClient) createExchange() error {
	if client.Exchange != "" {
		log.Printf("Creating exchange: %s\n", client.Exchange)
		err := client.Channel.ExchangeDeclare(
			client.Exchange,
			"direct",
			true,
			false,
			false,
			false,
			nil)
		if err != nil {
			log.Printf("Failed to declare exchange: %v\n", err)
		}
		return err
	}
	return nil
}

func (client *RabbitMQClient) createQueue() error {
	if client.Queue != "" {
		log.Printf("Creating queue: %s\n", client.Queue)
		_, err := client.Channel.QueueDeclare(
			client.Queue,
			true,
			false,
			false,
			false,
			nil)
		if err != nil {
			log.Printf("Failed to declare queue: %v\n", err)
		}
		return err
	}
	return nil
}

func (client *RabbitMQClient) bindQueueToExchange() error {
	log.Printf("Binding queue %s to exchange %s\n", client.Queue, client.Exchange)
	err := client.Channel.QueueBind(
		client.Queue,
		"",
		client.Exchange,
		false,
		nil)
	if err != nil {
		log.Printf("Failed to bind queue to exchange: %v\n", err)
	}
	return err
}

func (client *RabbitMQClient) Send(message Message) error {
	log.Println("Sending message...")

	if client.Connection == nil {
		log.Println("Connection does not exist.")
		return errors.New("connection does not exist")
	}

	if client.Channel == nil {
		log.Println("Channel does not exist.")
		return errors.New("channel does not exist")
	}

	body, err := client.Serializer.Marshal(message)
	if err != nil {
		log.Printf("Failed to serialize message: %v\n", err)
		return err
	}

	err = client.Channel.Publish(
		client.Exchange,
		"",
		false,
		false,
		amqp.Publishing{
			ContentType: "application/json",
			Body:        body,
		},
	)

	if err != nil {
		log.Printf("Failed to publish message: %v\n", err)
		return err
	}

	log.Println("Message sent successfully.")
	return nil
}

func (client *RabbitMQClient) Close() error {
	log.Println("Closing RabbitMQ connection...")

	if err := client.Channel.Close(); err != nil {
		log.Printf("Failed to close channel: %v\n", err)
		return err
	}

	if err := client.Connection.Close(); err != nil {
		log.Printf("Failed to close connection: %v\n", err)
		return err
	}

	log.Println("RabbitMQ connection closed successfully.")
	return nil
}
