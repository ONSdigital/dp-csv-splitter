package message

import (
	"encoding/json"
	"github.com/ONSdigital/dp-csv-splitter/aws"
	"github.com/ONSdigital/dp-csv-splitter/splitter"
	"github.com/ONSdigital/go-ns/log"
	"github.com/Shopify/sarama"
	"github.com/satori/go.uuid"
	"time"
)

func ConsumerLoop(listener Listener, awsService aws.AWSService, processor splitter.CSVProcessor) {
	for message := range listener.Messages() {
		log.Debug("Message received from Kafka!", nil)
		processMessage(message, awsService, processor)
	}
}

func processMessage(message *sarama.ConsumerMessage, awsService aws.AWSService, csvProcessor splitter.CSVProcessor) error {

	var fileUploadedEvent FileUploaded
	if err := json.Unmarshal(message.Value, &fileUploadedEvent); err != nil {
		log.Error(err, nil)
		return err
	}

	log.Debug("Message filename:"+fileUploadedEvent.Filename, nil)

	awsReader, err := awsService.GetCSV(fileUploadedEvent.Filename)
	if err != nil {
		log.Error(err, log.Data{"message": "Error while attempting get to get from from AWS s3 bucket."})
		return err
	}

	datasetId := uuid.NewV4().String()
	csvProcessor.Process(awsReader, fileUploadedEvent.Filename, time.Now(), datasetId)

	return nil
}

// FileUploaded event
type FileUploaded struct {
	Filename string `json:"filename"`
	Time     int64  `json:"time"`
}

type Listener interface {
	Messages() <-chan *sarama.ConsumerMessage
}
