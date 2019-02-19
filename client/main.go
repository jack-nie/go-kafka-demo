package main

import (
	"fmt"
	"github.com/Shopify/sarama"
)

func main() {
	config := sarama.NewConfig()
	config.Consumer.Return.Errors = true

	brokers := []string{"127.0.0.1:9093", "127.0.0.1:9094"}
	client, err := sarama.NewClient(brokers, config)
	if err != nil {
		panic(err)
	}
	fmt.Printf("brokers: %+v\n", client.Brokers())
	fmt.Printf("config: %+v\n", client.Config())

	partitionList, err := client.Partitions("my_topic")
	if err != nil {
		panic(err)
	}
	fmt.Printf("partitions: %+v\n", partitionList)

	topicList, err := client.Topics()
	if err != nil {
		panic(err)
	}
	fmt.Printf("topics: %+v\n", topicList)

	leader, err := client.Leader("my_topic", 0)
	if err != nil {
		panic(err)
	}
	fmt.Printf("leader: %+v\n", leader)
}
