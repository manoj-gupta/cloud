package kafkapc

import (
	"context"
	"fmt"
	"log"
	"os"
	"strconv"
	"time"

	"github.com/segmentio/kafka-go"
)

// the topic and broker addresses
const (
	topic          = "sample-kafka-topic"
	broker1Address = "localhost:9093"
	broker2Address = "localhost:9094"
	broker3Address = "localhost:9095"
)

// Producer ... writes a message into the Kafka cluster every second
func Producer(ctx context.Context) {
	count := 0

	l := log.New(os.Stdout, "Kafka producer: ", 0)
	// intialize the writer with the broker addresses, and the topic
	w := kafka.NewWriter(kafka.WriterConfig{
		Brokers: []string{broker1Address, broker2Address, broker3Address},
		Topic:   topic,
		Logger:  l, // assign the logger to the writer
	})

	for {
		// message is key:value pair. Key determines the partition to publish data
		err := w.WriteMessages(ctx, kafka.Message{
			Key:   []byte(strconv.Itoa(count)),
			Value: []byte("Sample message " + strconv.Itoa(count)),
		})

		if err != nil {
			panic("could not write message " + err.Error())
		}

		// log a confirmation once the message is written
		fmt.Println("written: ", count)
		count++

		// sleep for a second
		time.Sleep(time.Second)
	}
}

// Consumer ... consumes messages from the Kafka cluster
func Consumer(ctx context.Context) {
	l := log.New(os.Stdout, "Kafka consumer: ", 0)

	// groupID - identifies the consumer to receiving duplicates
	r := kafka.NewReader(kafka.ReaderConfig{
		Brokers: []string{broker1Address, broker2Address, broker3Address},
		Topic:   topic,
		GroupID: "sample-group",
		Logger:       l, // assign the logger to the reader
	})

	for {
		// the `ReadMessage` blocks until we receive the next event
		msg, err := r.ReadMessage(ctx)
		if err != nil {
			panic("could not read message " + err.Error())
		}
		// after receiving the message, log its value
		fmt.Println("received: ", string(msg.Value))
	}
}
