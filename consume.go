package main

import (
	"fmt"
	"github.com/Shopify/sarama"
	"os"
	"os/signal"
	"sync"
)

func consume() {
	c, err := sarama.NewConsumer(brokerList, nil)
	if err != nil {
		fmt.Printf("Failed to start consumer: %s\n", err)
		return
	}

	partitionList, err := c.Partitions(topic)
	if err != nil {
		fmt.Printf("Failed to get the list of partitions: %s\n", err)
	}

	var (
		messages = make(chan *sarama.ConsumerMessage, consumeChannelBufferSize)
		closing  = make(chan struct{})
		wg       sync.WaitGroup
	)

	// Capture any KILL or SIGTERM signals, notify all consumers to close
	go func() {
		signals := make(chan os.Signal, 1)
		signal.Notify(signals, os.Kill, os.Interrupt)
		<-signals
		fmt.Println("Initiating shutdown of consumer...")
		close(closing)
	}()

	for _, partition := range partitionList {
		pc, err := c.ConsumePartition(topic, partition, consumeOffset)
		if err != nil {
			fmt.Printf("Failed to start consumer for partition %d: %s\n", partition, err)
		}

		// Trigger closing all message channels via AsyncClose if closing signal received
		go func(pc sarama.PartitionConsumer) {
			<-closing
			pc.AsyncClose()
		}(pc)

		wg.Add(1)

		// Keep consuming from consumer's messages channel until it closes.
		go func(pc sarama.PartitionConsumer) {
			defer wg.Done()
			for message := range pc.Messages() {
				messages <- message
			}
		}(pc)
	}

	// Prints messages received from all of the partitionConsumer go-routines
	go func() {
		for msg := range messages {
			fmt.Printf("Partition:\t%d\n", msg.Partition)
			fmt.Printf("Offset:\t%d\n", msg.Offset)
			fmt.Printf("Key:\t%s\n", string(msg.Key))
			fmt.Printf("Value:\t%s\n", string(msg.Value))
			fmt.Println()
		}
	}()

	wg.Wait()
	close(messages)

	if err := c.Close(); err != nil {
		fmt.Printf("Failed to close consumer: %s\n", err)
	}
}
