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

	var batchSize = 100
	var batchNumber = 1

	var isFinalBatch bool = false;

	batchLoop:
	for {
		// each batch

		var msgs []*sarama.ProducerMessage = make([]*sarama.ProducerMessage, batchSize)

		log.Debug("Processing batch number " + strconv.Itoa(batchNumber) + " index: " + strconv.Itoa(index), nil)

		createBatchLoop:
		for batchIndex := 0; batchIndex < batchSize; batchIndex++ {
			// each row in the batch

			row, err := csvR.Read()
			if err != nil {
				if err == io.EOF {
					log.Debug("EOF reached, no more records to process", nil)
					isFinalBatch = true
					msgs = msgs[0:batchIndex] // the last batch is smaller than batch size, so resize the slice.

					log.Debug(strconv.Itoa(batchIndex) + " messages in this batch.", nil)
					break createBatchLoop
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

			msgs[batchIndex] = producerMsg

			index++
		}

		err := Producer.SendMessages(msgs)
		if err != nil {
			log.Error(err, log.Data{
				"details": "Failed to add messages to Kafka",
			})
		}

		if isFinalBatch {
			break batchLoop
		}

		batchNumber++
	}

	log.Debug("Kafka Loop details", log.Data{
		"Enqueued": index,
		"Errors":   errorCount,
	})
}

