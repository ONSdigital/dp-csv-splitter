package splitter

import (
	"bufio"
	"encoding/json"
	"github.com/ONSdigital/dp-csv-splitter/config"
	"github.com/ONSdigital/dp-csv-splitter/message/event"
	"github.com/ONSdigital/go-ns/log"
	"github.com/Shopify/sarama"
	"io"
	"strconv"
	"time"
)

var Producer sarama.SyncProducer

// CSVProcessor defines the CSVProcessor interface.
type CSVProcessor interface {
	Process(r io.Reader, uploadEvent event.UploadEvent, startTime time.Time, datasetID string)
}

// Processor implementation of the CSVProcessor interface.
type Processor struct{}

// NewCSVProcessor create a new Processor.
func NewCSVProcessor() *Processor {
	return &Processor{}
}

type Message struct {
	Index int    `json:"index"`
	Row   string `json:"row"`
	//Filename  string `json:"filename"`
	StartTime int64  `json:"startTime"`
	DatasetID string `json:"datasetID"`
	S3URL     string `json:"s3URL"`
}

func (p *Processor) Process(r io.Reader, fileUploaded event.UploadEvent, startTime time.Time, datasetID string) {

	scanner := bufio.NewScanner(r)
	var index = 0
	var batchSize = config.BatchSize
	var batchNumber = 1
	var isFinalBatch = false

	// Scan and discard header row (for now) - the data rows contain sufficient information about the structure
	if !scanner.Scan() && scanner.Err() == io.EOF {
		log.Debug("Encountered EOF immediately when processing header row", nil)
		return
	}

	for !isFinalBatch {
		// each batch

		log.Debug("Processing batch number "+strconv.Itoa(batchNumber)+" index: "+strconv.Itoa(index), nil)
		var msgs []*sarama.ProducerMessage = make([]*sarama.ProducerMessage, batchSize)

		for batchIndex := 0; batchIndex < batchSize && !isFinalBatch; batchIndex++ {
			// each row in the batch
			scanSuccessful := scanner.Scan()
			if !scanSuccessful {
				log.Debug("EOF reached, no more records to process", nil)
				isFinalBatch = true
				msgs = msgs[0:batchIndex] // the last batch is smaller than batch size, so resize the slice.
				log.Debug(strconv.Itoa(batchIndex)+" messages in the final batch.", nil)

			} else {
				producerMsg := createMessage(scanner.Text(), index, fileUploaded, startTime, datasetID)
				msgs[batchIndex] = producerMsg
				index++
			}
		}

		err := Producer.SendMessages(msgs)
		if err != nil {
			log.Error(err, log.Data{
				"details": "Failed to add messages to Kafka",
			})
		}

		batchNumber++
	}

	log.Debug("Kafka Loop details", log.Data{
		"Enqueued": index,
	})
}

func createMessage(row string, index int, fileUploaded event.UploadEvent, startTime time.Time, datasetID string) *sarama.ProducerMessage {

	message := Message{
		Index: index,
		Row:   row,
		//Filename:  fileUploaded.Filename,
		S3URL:     fileUploaded.GetS3URL(),
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
		Topic: config.TopicName,
		Key:   sarama.StringEncoder(strTime),
		Value: sarama.ByteEncoder(messageJSON),
	}

	return producerMsg
}
