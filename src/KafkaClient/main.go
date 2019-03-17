package main

import (
	"github.com/Shopify/sarama"
	"log"
	"os"
	"os/signal"
)

func init() {
	sarama.Logger = log.New(os.Stdout, "[Sarama] ", log.LstdFlags)
}

var (
	brokers   = "192.168.0.152:9092"
	logger = log.New(os.Stdout, "[kafkaclient] ", log.LstdFlags)
)

func main() {
	logger.Printf("main called")

	consumer, err := sarama.NewConsumer([]string{"192.168.0.152:9092"}, nil)
	if err != nil {
		panic(err)
	}

	partitionConsumer, err := consumer.ConsumePartition("test", 0, sarama.OffsetOldest)
	if err != nil {
		panic(err)
	}

	// Trap SIGINT to trigger a shutdown.
	signals := make(chan os.Signal, 1)
	signal.Notify(signals, os.Interrupt)

	consumed := 0
    ConsumerLoop:
	for {
		select {
		case msg := <-partitionConsumer.Messages():
			log.Printf("Consumed message offset %d %s\n", msg.Offset, msg.Value)
			consumed++
		case <-signals:
			break ConsumerLoop
		}
	}

	log.Printf("Consumed: %d\n", consumed)

	consumer.Close();
}

