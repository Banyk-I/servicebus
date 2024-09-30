package main

import (
	"github.com/ib407ov/servicebus"
	"log"
	"time"
)

// UserMessage представляє повідомлення, яке буде надіслано
type UserMessage struct {
	ID   int    `json:"id"`
	Name string `json:"name"`
}

// Реалізуємо метод GetRoutingKey для UserMessage
func (m UserMessage) GetRoutingKey() string {
	return "qweqwe" // Додано роутінг кей "test", але може бути будь-яким
}

func main() {
	// Параметри RabbitMQ
	amqpURL := "amqp://guest:guest@localhost:5672/"
	exchange := "test_exchange"
	queue := "test_queue"

	// Створюємо RabbitMQ клієнта
	client, err := servicebus.NewRabbitMQClient(amqpURL, exchange, queue)
	if err != nil {
		log.Fatalf("Failed to create RabbitMQ client: %v", err)
	}
	defer client.Close()

	// Створюємо повідомлення
	message := UserMessage{
		ID:   0,
		Name: "illia",
	}

	// Прив'язуємо чергу до exchange з використанням роутінг ключа повідомлення
	err = client.BindQueueToExchange(message.GetRoutingKey())
	if err != nil {
		log.Fatalf("Failed to bind queue to exchange: %v", err)
	}

	// Надсилаємо повідомлення з вказаним роутінг кейом
	err = client.Send(message)
	if err != nil {
		log.Fatalf("Failed to send message: %v", err)
	}

	log.Println("Message sent:", message)

	time.Sleep(15 * time.Second)
	// Споживаємо повідомлення
	err = client.Consume(func(msg servicebus.Message) {
		log.Printf("Received message: %+v", msg)
	})
	if err != nil {
		log.Fatalf("Failed to consume messages: %v", err)
	}
}
