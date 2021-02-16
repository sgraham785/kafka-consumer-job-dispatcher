package main

import (
	"flag"
	"os"
	"os/signal"
	"syscall"

	"github.com/sgraham785/kafka-consumer-job-dispatcher/internal"
)

func main() {
	var maxWorkers int
	var consumerBroker string
	var consumerGroupID string
	// var producerBroker string
	// var producerTopic string

	flag.IntVar(&maxWorkers, "w", 2, "Max number of workers")
	flag.StringVar(&consumerBroker, "b", "localhost:9092", "Address to consumer broker")
	flag.StringVar(&consumerGroupID, "g", "tester", "Consumer group.id")
	// flag.StringVar(&producerBroker, "p", "localhost:9092", "Address to producer broker")
	// flag.StringVar(&producerTopic, "t", "processed", "Topic that to produce results to")

	flag.Parse()

	sigchan := make(chan os.Signal, 1)
	signal.Notify(sigchan, syscall.SIGINT, syscall.SIGTERM)
	go func() {
		<-sigchan
		internal.KafkaConsumer.Stop(internal.KafkaConsumer{})
		internal.Worker.Stop(internal.Worker{})
		os.Exit(1)
	}()

	// Start the dispatcher
	dispatcher := internal.NewDispatcher(maxWorkers)
	dispatcher.Run()

	// Start consuming topics
	kafka := internal.NewKafkaConsumer(consumerBroker, consumerGroupID, flag.Args())
	kafka.ConsumeTopics()
}
