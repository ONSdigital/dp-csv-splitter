package splitter_test

import (
	"errors"
	"github.com/ONSdigital/dp-csv-splitter/splitter"
	"github.com/Shopify/sarama"
	"github.com/Shopify/sarama/mocks"
	. "github.com/smartystreets/goconvey/convey"
	"strings"
	"testing"
)

var exampleCsvLine string = "36929,,,,,,,,,,,,,,,,,2014,2014,,Year,,,,,,,,,,,,,,,NACE,NACE,,08,08 - Other mining and quarrying,,,,Prodcom Elements,Prodcom Elements,,UK manufacturer sales ID,UK manufacturer sales LABEL,,\n\n"

func TestProcessor(t *testing.T) {

	kafkaConfig := sarama.NewConfig()
	kafkaConfig.Producer.Retry.Max = 5
	kafkaConfig.Producer.RequiredAcks = sarama.WaitForLocal
	kafkaConfig.Producer.Return.Successes = true

	Convey("Given a mock producer with a single expected intput that succeeds", t, func() {
		mockProducer := mocks.NewSyncProducer(t, kafkaConfig)
		mockProducer.ExpectSendMessageAndSucceed()
		splitter.Producer = mockProducer

		var Processor = splitter.NewCSVProcessor()

		Convey("Given a reader with a single CSV line", func() {
			reader := strings.NewReader(exampleCsvLine)

			Convey("When the processor is called", func() {
				Processor.Process(reader)
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
				Processor.Process(reader)
			})
		})
	})

}
