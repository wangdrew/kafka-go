package main

import (
	"flag"
	"fmt"
	"github.com/Shopify/sarama"
	"os"
)

var (
	publishOrConsume = flag.String("mode", "", "Start as stdin publisher or topic consumer")
)

var (
	kafkaBroker1             = "192.168.99.100:32783"
	kafkaBroker2             = "192.168.99.100:32784"
	kafkaBroker3             = "192.168.99.100:32785"
	brokerList               = []string{kafkaBroker1, kafkaBroker2, kafkaBroker3}
	topic                    = "helix"
	partition                = -1
	consumeOffset            = sarama.OffsetNewest
	consumeChannelBufferSize = 256
)

func main() {
	flag.Parse()

	switch *publishOrConsume {
	case "publish":
		publish()
	case "consume":
		consume()
	default:
		fmt.Println("Choose either 'publish' or 'consume'")
		os.Exit(64)
	}
}
