package internal

import (
	"encoding/json"
	"fmt"
	"os"

	"github.com/confluentinc/confluent-kafka-go/kafka"
)

// KafkaConsumer represents the kafka consumer
type KafkaConsumer struct {
	Consumer *kafka.Consumer
	Topics   []string
	quit     chan bool
}

func NewKafkaConsumer(broker, group string, topics []string) KafkaConsumer {
	c, err := kafka.NewConsumer(&kafka.ConfigMap{
		"bootstrap.servers":               broker,
		"group.id":                        group,
		"session.timeout.ms":              6000,
		"go.events.channel.enable":        true,
		"go.application.rebalance.enable": true,
		// Enable generation of PartitionEOF when the
		// end of a partition is reached.
		"enable.partition.eof": true,
		"auto.offset.reset":    "earliest"})
	if err != nil {
		fmt.Fprintf(os.Stderr, "Failed to create consumer: %s\n", err)
		os.Exit(1)
	}

	return KafkaConsumer{
		Consumer: c,
		Topics:   topics,
		quit:     make(chan bool),
	}
}

func (k KafkaConsumer) ConsumeTopics() {

	fmt.Printf("Created Consumer %v\n", k.Consumer)

	err := k.Consumer.SubscribeTopics(k.Topics, nil)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Failed to subscribe to topics: %s\n", err)
		os.Exit(1)
	}

	run := true
	for run == true {
		select {
		case ok := <-k.quit:
			fmt.Printf("run = %v\n", ok)
			run = false
			return

		case ev := <-k.Consumer.Events():
			switch e := ev.(type) {
			case kafka.AssignedPartitions:
				fmt.Fprintf(os.Stderr, "%% %v\n", e)
				k.Consumer.Assign(e.Partitions)

			case kafka.RevokedPartitions:
				fmt.Fprintf(os.Stderr, "%% %v\n", e)
				k.Consumer.Unassign()

			case *kafka.Message:
				var payload Payload
				json.Unmarshal(e.Value, &payload)
				fmt.Printf("%% kafka Message id=%s :: type=%s\n",
					string(payload.ID), string(payload.Type))
				// let's create a job with the payload
				work := Job{Payload: payload}
				// Push the work onto the queue.
				JobQueue <- work

			case kafka.PartitionEOF:
				fmt.Printf("%% Reached %v\n", e)

			case kafka.Error:
				// Errors should generally be considered as informational, the client will try to automatically recover
				fmt.Fprintf(os.Stderr, "%% Error: %v\n", e)
			}
		}
	}

	fmt.Printf("Closing consumer\n")
	k.Consumer.Close()
}

// Stop signals the worker to stop listening for work requests.
func (k KafkaConsumer) Stop() {
	fmt.Printf("Consumer stop\n")
	go func() {
		k.quit <- true
	}()
}
