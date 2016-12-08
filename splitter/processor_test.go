package splitter_test

import (
	"errors"
	"github.com/ONSdigital/dp-csv-splitter/splitter"
	"github.com/Shopify/sarama"
	"github.com/Shopify/sarama/mocks"
	. "github.com/smartystreets/goconvey/convey"
	"log"
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
		mockProducer := mocks.NewAsyncProducer(t, kafkaConfig)
		mockProducer.ExpectInputAndSucceed()
		splitter.Producer = mockProducer

		var Processor = splitter.NewCSVProcessor()

		Convey("Given a reader with a single CSV line", func() {
			reader := strings.NewReader(exampleCsvLine)

			Convey("When the processor is called", func() {
				Processor.Process(reader, filename, startTime, datasetID)
				message := <-mockProducer.Successes()
				So("test", ShouldEqual, message.Topic)
				So(1, ShouldEqual, message.Offset)
			})
		})
	})

	Convey("Given a mock producer with a single expected intput that fails", t, func() {
		mockProducer := mocks.NewAsyncProducer(t, kafkaConfig)
		mockProducer.ExpectInputAndFail(errors.New(""))
		splitter.Producer = mockProducer

		var Processor = splitter.NewCSVProcessor()

		Convey("Given a reader with a single CSV line", func() {
			reader := strings.NewReader(exampleCsvLine)

			Convey("When the processor is called", func() {
				Processor.Process(reader, filename, startTime, datasetID)
				err := <-mockProducer.Errors()
				log.Print(err)
			})
		})
	})

}
