package main

import (
	"fmt"
	"github.com/Shopify/sarama"
	"os"
	"os/signal"
	"strconv"
	"time"
)

func main() {
	config := sarama.NewConfig()
	config.Producer.RequiredAcks = sarama.WaitForAll
	config.Producer.Retry.Max = 5

	brokers := []string{"localhost:9093", "localhost:9094"}
	producer, err := sarama.NewAsyncProducer(brokers, config)
	if err != nil {
		panic(err)
	}

	defer func() {
		if err := producer.Close(); err != nil {
			panic(err)
		}
	}()
	signals := make(chan os.Signal, 1)
	signal.Notify(signals, os.Interrupt)

	var enqueued, errors int
	doneCh := make(chan struct{})
	go func() {
		for {
			time.Sleep(500 * time.Millisecond)
			strTime := strconv.Itoa(int(time.Now().Unix()))
			msg := &sarama.ProducerMessage{
				Topic: "my_topic",
				Key:   sarama.StringEncoder(strTime),
				Value: sarama.StringEncoder("something cool"),
			}

			select {
			case producer.Input() <- msg:
				enqueued++
				fmt.Println("produce message")
			case err := <-producer.Errors():
				errors++
				fmt.Println("Failed to produce message: ", err)
			case <-signals:
				doneCh <- struct{}{}
			}
		}
	}()
	<-doneCh
	fmt.Printf("Enqueued: %d, errors: %d\n", enqueued, errors)
}
