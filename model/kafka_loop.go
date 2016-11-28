package model

import (
	"github.com/Shopify/sarama"
	"encoding/csv"
	"io"
	"fmt"
	"os"
	"strings"
	"encoding/json"
	"strconv"
	"time"
	"log"
	"os/signal"
)

func Loop(csvr *csv.Reader, producer sarama.AsyncProducer) {
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
			select {
			case producer.Input() <- msg:
				enqueued++
				fmt.Println("Produce message", msg_json)
			case err := <- producer.Errors():
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
