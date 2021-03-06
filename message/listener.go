package message

import (
	"encoding/json"
	"time"

	"github.com/ONSdigital/dp-csv-splitter/ons_aws"
	"github.com/ONSdigital/dp-csv-splitter/message/event"
	"github.com/ONSdigital/dp-csv-splitter/splitter"
	"github.com/ONSdigital/go-ns/log"
	"github.com/Shopify/sarama"
	"github.com/satori/go.uuid"
)

func ConsumerLoop(listener Listener, awsService ons_aws.AWSService, processor splitter.CSVProcessor) {
	for message := range listener.Messages() {
		log.Debug("Message received from Kafka!", nil)
		processMessage(message, awsService, processor)
	}
}

func processMessage(message *sarama.ConsumerMessage, awsService ons_aws.AWSService, csvProcessor splitter.CSVProcessor) error {

	var event event.FileUploaded
	if err := json.Unmarshal(message.Value, &event); err != nil {
		log.Error(err, nil)
		return err
	}

	log.Debug("Processing uploadEvent message", log.Data{"url": event.GetURL()})

	awsReadCloser, err := awsService.GetCSV(&event)
	defer awsReadCloser.Close()
	if err != nil {
		log.Error(err, log.Data{"message": "Error while attempting get to get from from AWS s3 bucket."})
		return err
	}

	datasetId := uuid.NewV4().String()
	csvProcessor.Process(awsReadCloser, &event, time.Now(), datasetId)
	return nil
}

type Listener interface {
	Messages() <-chan *sarama.ConsumerMessage
}
