package main

import (
	"fmt"
	"github.com/Shopify/sarama"
	"os"
	"bufio"
)

func publish() {
	config := sarama.NewConfig()
	config.Producer.RequiredAcks = sarama.WaitForAll
	config.Producer.Return.Successes = true
	config.Producer.Partitioner = sarama.NewHashPartitioner

	stdinReader := bufio.NewReader(os.Stdin)

	producer, err := sarama.NewSyncProducer(brokerList, config)
	if err != nil {
		fmt.Printf("Failed to open Kafka producer: %s\n", err)
		return
	}
	defer func() {
		if err := producer.Close(); err != nil {
			fmt.Printf("Failed to close Kafka producer cleanly: %s\n", err)
			return
		}
	}()

	for {
		message := &sarama.ProducerMessage{Topic: topic, Partition: int32(partition)}
		text, err := stdinReader.ReadString('\n')
		if err != nil {
			fmt.Printf("Failed to read from stdin: %s\n", err)
			return
		}

		message.Value = sarama.StringEncoder(text)

		partition, offset, err := producer.SendMessage(message)
		if err != nil {
			fmt.Printf("Failed to produce message: %s\n", err)
			return
		}

		fmt.Printf("topic=%s\tpartition=%d\toffset=%d\n", topic, partition, offset)
	}
}
