package main

import (
	"encoding/json"
	"fmt"
	"os"
	"os/signal"
	"syscall"
	"time"

	"go-rabbitmq/classic/rabbitmq"

	amqp "github.com/rabbitmq/amqp091-go"
)

type Payload struct {
	AccountID string `json:"account_id"`
	Amount    uint32 `json:"amount"`
}

func forever() {
	for {
		time.Sleep(time.Second)
	}
}

func main() {
	broker, err := amqp.Dial("amqp://localhost")
	if err != nil {
		return
	}
	defer broker.Close()

	consumer := rabbitmq.NewConsumer(broker, "dev", "consumer2")
	exchangeName := "account.transaction.withdraw.321"
	err = consumer.ConsumeFanout(exchangeName, func(msg amqp.Delivery) error {
		var payload Payload

		err := json.Unmarshal(msg.Body, &payload)
		if err != nil {
			return err
		}

		// return errors.New("new error")

		fmt.Printf("[X] Consumer 2 received a message from %s: %+v\n", msg.Exchange, payload)
		fmt.Println("[X] Consumer 2 waiting for message. To exit press CTRL+C")

		return nil
	})
	if err != nil {
		fmt.Println(err.Error())
		return
	}

	fmt.Println("[X] Consumer 2 waiting for message. To exit press CTRL+C")

	go forever()
	quitChannel := make(chan os.Signal, 1)
	signal.Notify(quitChannel, syscall.SIGINT, syscall.SIGTERM)
	<-quitChannel
}
