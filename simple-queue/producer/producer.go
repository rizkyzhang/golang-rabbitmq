package main

import (
	"context"
	"fmt"

	"go-rabbitmq/simple-queue/rabbitmq"

	amqp "github.com/rabbitmq/amqp091-go"
)

type User struct {
	Name string
	Age  int
}

func main() {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	broker, err := amqp.Dial("amqp://localhost")
	if err != nil {
		return
	}
	defer broker.Close()

	producer := rabbitmq.NewProducer(ctx, broker)
	payload := &User{
		Name: "Willy",
		Age:  23,
	}

	err = producer.Publish("test.queue", payload)
	if err != nil {
		fmt.Println(err.Error())
		return
	}

	fmt.Printf("[X] Publish to default exchange with message %+v", payload)
}
