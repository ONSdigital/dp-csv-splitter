package splitter_test

import (
	"testing"
	. "github.com/smartystreets/goconvey/convey"
	"github.com/Shopify/sarama/mocks"
	"github.com/Shopify/sarama"
	"github.com/ONSdigital/dp-csv-splitter/splitter"
	"strings"
	"errors"
	"log"
)

var exampleCsvLine string = "36929,,,,,,,,,,,,,,,,,2014,2014,,Year,,,,,,,,,,,,,,,NACE,NACE,,08,08 - Other mining and quarrying,,,,Prodcom Elements,Prodcom Elements,,UK manufacturer sales ID,UK manufacturer sales LABEL,,\n\n"

func TestProcessor(t *testing.T) {

	kafkaConfig := sarama.NewConfig()
	kafkaConfig.Producer.Retry.Max = 5
	kafkaConfig.Producer.RequiredAcks = sarama.WaitForLocal
	kafkaConfig.Producer.Return.Successes = true

	Convey("Given a mock producer with a single expected intput that succeeds", t, func() {
		mockProducer := mocks.NewAsyncProducer(t, kafkaConfig)
		mockProducer.ExpectInputAndSucceed();
		splitter.Producer = mockProducer

		var Processor = splitter.NewCSVProcessor()

		Convey("Given a reader with a single CSV line", func() {
			reader := strings.NewReader(exampleCsvLine)

			Convey("When the processor is called", func() {
				Processor.Process(reader)
				message := <-mockProducer.Successes()
				So("test", ShouldEqual, message.Topic)
				So(1, ShouldEqual, message.Offset)
			})
		})
	})

	Convey("Given a mock producer with a single expected intput that fails", t, func() {
		mockProducer := mocks.NewAsyncProducer(t, kafkaConfig)
		mockProducer.ExpectInputAndFail(errors.New(""));
		splitter.Producer = mockProducer

		var Processor = splitter.NewCSVProcessor()

		Convey("Given a reader with a single CSV line", func() {
			reader := strings.NewReader(exampleCsvLine)

			Convey("When the processor is called", func() {
				Processor.Process(reader)
				err := <-mockProducer.Errors()
				log.Print(err)
			})
		})
	})

}