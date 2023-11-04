package main

import (
	"context"
	"fmt"

	"github.com/aws/aws-sdk-go-v2/config"
	kafka "github.com/segmentio/kafka-go"
	aws "github.com/segmentio/kafka-go/sasl/aws_msk_iam_v2"
)

func main() {
	cfg, err := config.LoadDefaultConfig(context.TODO())
	if err != nil {
		panic(err)
	}

	mechanism := aws.NewMechanism(cfg)

	dialer := kafka.DefaultDialer
	dialer.SASLMechanism = mechanism

	r := kafka.NewReader(kafka.ReaderConfig{
		Brokers:   []string{"localhost:9092"},
		Topic:     "dogs",
		Partition: 0,
		MinBytes:  10e3, // 10KB
		MaxBytes:  10e6, // 10MB
		Dialer:    dialer,
	})

	ctx := context.Background()

	for {
		msg, err := r.ReadMessage(ctx)
		if err != nil {
			fmt.Printf("error while receiving message: %s\n", err.Error())
			break
		}
		fmt.Printf("message at offset %d: %s = %s\n", msg.Offset, string(msg.Key), string(msg.Value))
	}

	if err := r.Close(); err != nil {
		fmt.Printf("error while closing reader: %s\n", err.Error())
	}
}
