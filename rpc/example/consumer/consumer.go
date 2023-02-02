package main

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"os/signal"
	"syscall"
	"time"

	model "go-rabbitmq/rpc"
	"go-rabbitmq/rpc/rabbitmq"

	amqp "github.com/rabbitmq/amqp091-go"
)

type Payload struct {
	Input string `json:"input"`
}

type ResponseData struct {
	Answer int `json:"answer"`
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

	rpcQueueName := "rpc.test"
	consumer := rabbitmq.NewConsumer(broker, "dev")
	err = consumer.Consume(ctx, rpcQueueName, func(msg amqp.Delivery) ([]byte, error) {
		var res model.RPCResponse
		var payload Payload

		err := json.Unmarshal(msg.Body, &payload)
		if err != nil {
			res = model.RPCResponse{
				Status: "failed",
				Error:  err.Error(),
			}

			resByte, _ := json.Marshal(res)
			return resByte, nil
		}

		resData := ResponseData{
			Answer: 3,
		}
		resDataByte, _ := json.Marshal(resData)
		res = model.RPCResponse{
			Status: "success",
			Data:   resDataByte,
		}
		resByte, _ := json.Marshal(res)

		fmt.Println("[X] Consumer waiting for message. To exit press CTRL+C")

		return resByte, nil
	})
	if err != nil {
		fmt.Println(err.Error())
		return
	}

	go forever()
	quitChannel := make(chan os.Signal, 1)
	signal.Notify(quitChannel, syscall.SIGINT, syscall.SIGTERM)
	<-quitChannel
}
