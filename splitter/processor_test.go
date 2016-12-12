package splitter_test

import (
	"encoding/json"
	"errors"
	"github.com/ONSdigital/dp-csv-splitter/splitter"
	"github.com/Shopify/sarama"
	"github.com/Shopify/sarama/mocks"
	. "github.com/smartystreets/goconvey/convey"
	"strings"
	"testing"
	"time"
)

var exampleCsvLine string = "153223,,Person,,Count,,,,,,,,,,K04000001,,,,,,,,,,,,,,,,,,,,,Sex,Sex,,All categories: Sex,All categories: Sex,,,,Age,Age,,All categories: Age 16 and over,All categories: Age 16 and over,,,,Residence Type,Residence Type,,All categories: Residence Type,All categories: Residence Type,,,"

func TestProcessor(t *testing.T) {

	kafkaConfig := sarama.NewConfig()
	kafkaConfig.Producer.Retry.Max = 5
	kafkaConfig.Producer.RequiredAcks = sarama.WaitForLocal
	kafkaConfig.Producer.Return.Successes = true

	filename := "exampleFilename.csv"
	startTime := time.Now()
	datasetID := "werqae-asdqwrwf-erwe"

	Convey("Given a mock producer with a single expected intput that succeeds", t, func() {

		mockProducer := mocks.NewSyncProducer(t, kafkaConfig)
		mockProducer.ExpectSendMessageWithCheckerFunctionAndSucceed(func(val []byte) error {

			var message *splitter.Message
			json.Unmarshal(val, &message)

			if message.DatasetID != datasetID {
				return errors.New("Dataset ID was not added to the message.")
			}
			if message.Filename != filename {
				return errors.New("CSV filename was not added to the message.")
			}
			if message.StartTime != startTime.UTC().Unix() {
				return errors.New("Start time was not added to the message.")
			}
			if message.Index != 0 {
				return errors.New("Index was not added to the message.")
			}
			if message.Row != exampleCsvLine {
				return errors.New("CSV row was not added to the message.")
			}

			return nil
		})

		splitter.Producer = mockProducer

		var Processor = splitter.NewCSVProcessor()

		Convey("Given a reader with a single CSV line", func() {
			reader := strings.NewReader(exampleCsvLine)

			Convey("When the processor is called", func() {
				Processor.Process(reader, filename, startTime, datasetID)

			})
		})
	})

	Convey("Given a mock producer with a single expected intput that fails", t, func() {
		mockProducer := mocks.NewSyncProducer(t, kafkaConfig)
		mockProducer.ExpectSendMessageAndFail(errors.New(""))
		splitter.Producer = mockProducer

		var Processor = splitter.NewCSVProcessor()

		Convey("Given a reader with a single CSV line", func() {
			reader := strings.NewReader(exampleCsvLine)

			Convey("When the processor is called", func() {
				Processor.Process(reader, filename, startTime, datasetID)
			})
		})
	})
}
