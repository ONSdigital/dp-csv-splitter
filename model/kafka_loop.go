package model

import (
	"encoding/csv"
	"encoding/json"
	"fmt"
	"github.com/Shopify/sarama"
	"io"
	"log"
	"os"
	"strconv"
	"strings"
	"time"
)

func Loop(csvr *csv.Reader, producer sarama.SyncProducer) {

	var enqueued, errors int
	start := time.Now()
	for {

		row, err := csvr.Read()
		if err != nil {
			if err == io.EOF {
				elapsed := time.Since(start)
				log.Printf("Enqueued: %d; errors: %d\n", enqueued, errors)
				fmt.Println("EOF reached, no more records to process", elapsed.String())
				os.Exit(0)
			} else {
				fmt.Println("Error occored and cannot process anymore entry", err.Error())
				panic(err)
			}
		}

		msg_json := Message{Index: enqueued, Row: strings.Join(row[:], ",")}
		j, err := json.Marshal(msg_json)

		if err != nil {
			fmt.Printf("Could not create the json representation of message %s", msg_json)
			panic(err)
		}

		strTime := strconv.Itoa(int(time.Now().Unix()))
		msg := &sarama.ProducerMessage{
			Topic: "test",
			Key:   sarama.StringEncoder(strTime),
			Value: sarama.ByteEncoder(j),
		}

		_, _, err = producer.SendMessage(msg)
		if err != nil {
			panic(err)
		}

		enqueued++
	}

	log.Printf("Enqueued: %d; errors: %d\n", enqueued, errors)
}

func Producer(address string) sarama.SyncProducer {
	config := sarama.NewConfig()
	// Return specifies what channels will be populated.
	// If they are set to true, you must read from
	// config.Producer.Return.Successes = true
	// The total number of times to retry sending a message (default 3).
	config.Producer.Retry.Max = 5
	config.Producer.RequiredAcks = sarama.WaitForAll
	brokers := []string{address}
	producer, err := sarama.NewSyncProducer(brokers, config)
	if err != nil {
		panic(err)
	}

	return producer
}
