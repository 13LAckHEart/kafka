package main

import (
	"fmt"
	"log"

	"github.com/Shopify/sarama"
)

func main() {
	// Define the Kafka broker addresses
	brokers := []string{"localhost:9092"} // Adjust if needed (e.g., use the IP or hostname of your Kafka instance)

	// Create a new Sarama config
	config := sarama.NewConfig()
	config.Producer.RequiredAcks = sarama.WaitForAll // Wait for all in-sync replicas to acknowledge the message
	config.Producer.Retry.Max = 5                    // Retry up to 5 times if sending fails
	config.Producer.Return.Successes = true          // Return successful sends

	// Create a new Kafka producer
	producer, err := sarama.NewSyncProducer(brokers, config)
	if err != nil {
		log.Fatalf("Failed to create Kafka producer: %v", err)
	}
	defer producer.Close()

	// Prepare the message to send
	topic := "test-topic" // Make sure the topic exists in your Kafka cluster
	messageValue := "Hello, Kafka with KRaft from Go!"

	message := &sarama.ProducerMessage{
		Topic: topic,
		Value: sarama.StringEncoder(messageValue),
	}

	// Send the message
	partition, offset, err := producer.SendMessage(message)
	if err != nil {
		log.Fatalf("Failed to send message: %v", err)
	}

	fmt.Printf("Message sent to partition %d at offset %d\n", partition, offset)
}
