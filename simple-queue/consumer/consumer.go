package main

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"os/signal"
	"syscall"
	"time"

	"go-rabbitmq/simple-queue/rabbitmq"

	amqp "github.com/rabbitmq/amqp091-go"
)

type User struct {
	Name string
	Age  int
}

func forever() {
	for {
		time.Sleep(time.Second)
	}
}

func main() {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	broker, err := amqp.Dial("amqp://localhost")
	if err != nil {
		return
	}
	defer broker.Close()

	consumer := rabbitmq.NewConsumer(ctx, broker)
	queueName := "test.queue"

	err = consumer.Consume(queueName, func(msg amqp.Delivery) error {
		var payload User

		err := json.Unmarshal(msg.Body, &payload)
		if err != nil {
			return err
		}

		// return errors.New("new error")

		fmt.Printf("[X] Received a message from %s: %+v\n", queueName, payload)
		fmt.Println("[X] Waiting for message. To exit press CTRL+C")

		return nil
	})
	if err != nil {
		fmt.Println(err.Error())
		return
	}

	fmt.Println("[X] Waiting for message. To exit press CTRL+C")

	go forever()
	quitChannel := make(chan os.Signal, 1)
	signal.Notify(quitChannel, syscall.SIGINT, syscall.SIGTERM)
	<-quitChannel
}
