package main

import (
	"fmt"
	"github.com/Shopify/sarama"
)

var (
	kafkaBroker1 = "192.168.99.100:32768"
	kafkaBroker2 = "192.168.99.100:32769"
	kafkaBroker3 = "192.168.99.100:32770"
	brokerList   = []string{kafkaBroker1, kafkaBroker2, kafkaBroker3}
	topic        = "testTopic"
	partition    = -1
)

func main() {

	config := sarama.NewConfig()
	config.Producer.RequiredAcks = sarama.WaitForAll
	config.Producer.Return.Successes = true
	config.Producer.Partitioner = sarama.NewHashPartitioner

	message := &sarama.ProducerMessage{Topic: topic, Partition: int32(partition)}
	message.Value = sarama.StringEncoder("BLahBlahBlah")

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

	partition, offset, err := producer.SendMessage(message)
	if err != nil {
		fmt.Printf("Failed to produce message: %s\n", err)
		return
	}

	fmt.Printf("topic=%s\tpartition=%d\toffset=%d\n", topic, partition, offset)
	return
}
