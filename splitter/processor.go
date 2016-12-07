package splitter

import (
	"github.com/Shopify/sarama"
	"io"
	"github.com/ONSdigital/dp-csv-splitter/config"
	"github.com/ONSdigital/go-ns/log"
	"encoding/csv"
	"fmt"
	"strings"
	"encoding/json"
	"strconv"
	"time"
)

var Producer sarama.SyncProducer

// CSVProcessor defines the CSVProcessor interface.
type CSVProcessor interface {
	Process(r io.Reader)
}

// Processor implementation of the CSVProcessor interface.
type Processor struct{}

// NewCSVProcessor create a new Processor.
func NewCSVProcessor() *Processor {
	return &Processor{}
}

type Message struct {
	Index int    `json:"index"`
	Row   string `json:"datapoint"`
}

func createMessage(index int, row []string) Message {
	return Message{Index: index, Row: strings.Join(row[:], ",")}
}

func (p *Processor) Process(r io.Reader) {

		csvR := csv.NewReader(r)
		var index = 0
		var errorCount = 0

		csvLoop:
		for {
			row, err := csvR.Read()
			if err != nil {
				if err == io.EOF {
					log.Debug("EOF reached, no more records to process", nil)
					break csvLoop
				} else {
					fmt.Println("Error occored and cannot process anymore entry", err.Error())
					panic(err)
				}
			}

			messageJSON, err := json.Marshal(createMessage(index, row))

			if err != nil {
				log.Error(err, log.Data{
					"details": "Could not create the json representation of message",
					"message": messageJSON,
				})
				panic(err)
			}

			strTime := strconv.Itoa(int(time.Now().Unix()))
			producerMsg := &sarama.ProducerMessage{
				Topic: config.TopicName,
				Key:   sarama.StringEncoder(strTime),
				Value: sarama.ByteEncoder(messageJSON),
			}

			partition, offset, err := Producer.SendMessage(producerMsg)
			if err != nil {
				log.Error(err, log.Data{
					"details": "Failed to add message to Kafka",
					"message": messageJSON,
				})
			}

			log.Debug("Message sent", log.Data{
				"Partition": partition,
				"Offset": offset,
			})
		}

		log.Debug("Kafka Loop details", log.Data{
			"Enqueued": index,
			"Errors":   errorCount,
		})

}



