package splitter

import (
	"encoding/csv"
	"encoding/json"
	"fmt"
	"github.com/ONSdigital/dp-csv-splitter/config"
	"github.com/ONSdigital/go-ns/log"
	"github.com/Shopify/sarama"
	"io"
	"strconv"
	"strings"
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
	var batchSize = config.BatchSize
	var batchNumber = 1
	var isFinalBatch = false

batchLoop:
	for { // each batch

		log.Debug("Processing batch number "+strconv.Itoa(batchNumber)+" index: "+strconv.Itoa(index), nil)
		var msgs []*sarama.ProducerMessage = make([]*sarama.ProducerMessage, batchSize)

	createBatchLoop:
		for batchIndex := 0; batchIndex < batchSize; batchIndex++ { // each row in the batch

			row, err := csvR.Read()
			if err != nil {
				if err == io.EOF {
					log.Debug("EOF reached, no more records to process", nil)
					isFinalBatch = true
					msgs = msgs[0:batchIndex] // the last batch is smaller than batch size, so resize the slice.
					log.Debug(strconv.Itoa(batchIndex)+" messages in the final batch.", nil)
					break createBatchLoop
				} else {
					fmt.Println("Error occored and cannot process anymore entry", err.Error())
					panic(err)
				}
			}

			producerMsg := createMessageFromRow(row, index)
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
	})
}
func createMessageFromRow(row []string, index int) *sarama.ProducerMessage {
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

	return producerMsg
}
