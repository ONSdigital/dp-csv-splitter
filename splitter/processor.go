package splitter

import (
	"bufio"
	"encoding/json"
	"io"
	"strconv"
	"time"

	"github.com/ONSdigital/dp-csv-splitter/config"
	"github.com/ONSdigital/dp-csv-splitter/message/event"
	"github.com/ONSdigital/go-ns/log"
	"github.com/Shopify/sarama"
)

var Producer sarama.SyncProducer

// CSVProcessor defines the CSVProcessor interface.
type CSVProcessor interface {
	Process(r io.Reader, event *event.FileUploaded, startTime time.Time, datasetID string)
}

// Processor implementation of the CSVProcessor interface.
type Processor struct{}

// NewCSVProcessor create a new Processor.
func NewCSVProcessor() *Processor {
	return &Processor{}
}

type RowMessage struct {
	Index     int    `json:"index"`
	Row       string `json:"row"`
	StartTime int64  `json:"startTime"`
	DatasetID string `json:"datasetID"`
	S3URL     string `json:"s3URL"`
}

type DatasetSplitEvent struct {
	DatasetID     string `json:"datasetID"`
	TotalRows     int    `json:"totalRows"`
	SplitTime     int64  `json:"lastUpdate"`
}

func (p *Processor) Process(r io.Reader, event *event.FileUploaded, startTime time.Time, datasetID string) {

	scanner := bufio.NewScanner(r)
	var index = 0
	var batchSize = config.BatchSize
	var batchNumber = 1
	var isFinalBatch = false
	var totalRows int

	// Scan and discard header row (for now) - the data rows contain sufficient information about the structure
	if !scanner.Scan() && scanner.Err() == io.EOF {
		log.DebugC(datasetID,"Encountered EOF immediately when processing header row", nil)
		return
	}

	for !isFinalBatch {
		// each batch

		log.DebugC(datasetID, "Processing batch number "+strconv.Itoa(batchNumber)+" index: "+strconv.Itoa(index), nil)
		var msgs []*sarama.ProducerMessage = make([]*sarama.ProducerMessage, batchSize)

		for batchIndex := 0; batchIndex < batchSize && !isFinalBatch; batchIndex++ {
			// each row in the batch
			scanSuccessful := scanner.Scan()
			if !scanSuccessful {
				log.DebugC(datasetID, "EOF reached, no more records to process", nil)
				isFinalBatch = true
				msgs = msgs[0:batchIndex] // the last batch is smaller than batch size, so resize the slice.
				log.Debug(strconv.Itoa(batchIndex)+" messages in the final batch.", nil)
				totalRows = ((batchNumber - 1) * batchSize) + batchIndex
				log.DebugC(datasetID, strconv.Itoa(totalRows)+" messages in total.", nil)
				sendDatasetSplitEvent(datasetID, totalRows)
			} else {
				producerMsg := createMessage(scanner.Text(), index, event, startTime, datasetID)
				msgs[batchIndex] = producerMsg
				index++
			}
		}

		err := Producer.SendMessages(msgs)
		if err != nil {
			log.ErrorC(datasetID, err, log.Data{
				"details": "Failed to add messages to Kafka",
			})
		}

		batchNumber++
	}

	log.DebugC(datasetID, "Kafka Loop details", log.Data{
		"Enqueued": index,
	})
}

func sendDatasetSplitEvent(datasetID string, totalRows int) {

	message := DatasetSplitEvent{
		DatasetID: datasetID,
		TotalRows: totalRows,
		SplitTime: time.Now().UTC().Unix() * 1000, // unix time in milliseconds
	}

	messageJSON, err := json.Marshal(message)
	if err != nil {
		log.Error(err, log.Data{
			"details": "Could not create the json representation of message",
			"message": messageJSON,
		})
		panic(err)
	}

	producerMsg := &sarama.ProducerMessage{
		Topic: config.DatasetTopicName,
		Key:   sarama.StringEncoder(datasetID),
		Value: sarama.ByteEncoder(messageJSON),
	}

	log.Debug("Sending dataset status message", log.Data{"message": messageJSON})
	_, _, err = Producer.SendMessage(producerMsg)
	if err != nil {
		log.Error(err, log.Data{
			"details": "Failed to add messages to Kafka",
		})
	}
}

func createMessage(row string, index int, event *event.FileUploaded, startTime time.Time, datasetID string) *sarama.ProducerMessage {

	message := RowMessage{
		Index:     index,
		Row:       row,
		S3URL:     event.GetURL(),
		StartTime: startTime.UTC().Unix(),
		DatasetID: datasetID,
	}

	messageJSON, err := json.Marshal(message)

	if err != nil {
		log.Error(err, log.Data{
			"details": "Could not create the json representation of message",
			"message": messageJSON,
		})
		panic(err)
	}

	strTime := strconv.Itoa(int(time.Now().Unix()))
	producerMsg := &sarama.ProducerMessage{
		Topic: config.RowTopicName,
		Key:   sarama.StringEncoder(strTime),
		Value: sarama.ByteEncoder(messageJSON),
	}

	return producerMsg
}
