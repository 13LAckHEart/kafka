package main

import (
	"fmt"
	"log"

	"github.com/Shopify/sarama"
)

func main() {
	// Define Kafka broker addresses (adjust if necessary)
	brokers := []string{"localhost:9092"}
	topic := "test-topic" // Replace with your Kafka topic name

	// Create a new Sarama configuration
	config := sarama.NewConfig()
	config.Version = sarama.V2_6_0_0                      // Set the Kafka version according to your broker version
	config.Consumer.Offsets.Initial = sarama.OffsetOldest // Start from the oldest message

	// Create a new consumer
	consumer, err := sarama.NewConsumer(brokers, config)
	if err != nil {
		log.Fatalf("Error creating Kafka consumer: %v", err)
	}
	defer consumer.Close()

	// Subscribe to the topic's partition
	partitionConsumer, err := consumer.ConsumePartition(topic, 0, sarama.OffsetNewest)
	if err != nil {
		log.Fatalf("Error consuming Kafka partition: %v", err)
	}
	defer partitionConsumer.Close()

	fmt.Println("Kafka consumer started, waiting for messages...")

	// Consume messages in a loop
	for message := range partitionConsumer.Messages() {
		fmt.Printf("Consumed message: %s, partition: %d, offset: %d\n", string(message.Value), message.Partition, message.Offset)
	}
}
