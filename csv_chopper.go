package main

import (
	"fmt"
	"log"
	"os"
	"os/signal"
	"time"

	"strconv"

	"github.com/Shopify/sarama"
	"./model"
	"encoding/csv"
	"io"
	"encoding/json"
)

const usage = "Usage: ./csv_chopper <csv_file>"

func main() {

	if len(os.Args) != 2 {
		log.Fatal(usage)
	}

	csv_location := os.Args[1]
	f, err := os.Open(csv_location)
	if err != nil {
		fmt.Printf("Could not open the csv file %s, %s", csv_location, err.Error())
		os.Exit(1)
	}

	defer f.Close()

	csvr := csv.NewReader(f)

	config := sarama.NewConfig()
	// Return specifies what channels will be populated.
	// If they are set to true, you must read from
	// config.Producer.Return.Successes = true
	// The total number of times to retry sending a message (default 3).
	config.Producer.Retry.Max = 5
	config.Producer.RequiredAcks = sarama.WaitForAll
	brokers := []string{"localhost:9092"}
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

			row, err := csvr.Read()
			if err != nil {
				if err == io.EOF {
					fmt.Println("EOF reached, no more records to process")
					os.Exit(0)
				} else {
					fmt.Println("Error occored and cannot process anymore entry")
					panic(err)
				}
			}

			msg_json := model.Message{enqueued, row}
			j, err := json.Marshal(msg_json)

			if err != nil {
				fmt.Printf("Could not create the json representation of message %s", msg_json)
				panic(err)
			}

			strTime := strconv.Itoa(int(time.Now().Unix()))
			msg := &sarama.ProducerMessage{
				Topic: "",
				Key:   sarama.StringEncoder(strTime),
				Value: sarama.ByteEncoder(j),
			}
			select {
			case producer.Input() <- msg:
				enqueued++
				fmt.Println("Produce message")
			case err := <-producer.Errors():
				errors++
				fmt.Println("Failed to produce message:", err)
			case <-signals:
				doneCh <- struct{}{}
			}
		}
	}()

	<-doneCh
	log.Printf("Enqueued: %d; errors: %d\n", enqueued, errors)
}