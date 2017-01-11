package splitter_test

import (
	"encoding/json"
	"errors"
	"github.com/ONSdigital/dp-csv-splitter/message/event"
	"github.com/ONSdigital/dp-csv-splitter/splitter"
	"github.com/Shopify/sarama"
	"github.com/Shopify/sarama/mocks"
	. "github.com/smartystreets/goconvey/convey"
	"net/url"
	"strings"
	"testing"
	"time"
)

var exampleHeaderLine string = "Observation,Data_Marking,Statistical_Unit_Eng,Statistical_Unit_Cym,Measure_Type_Eng,Measure_Type_Cym,Observation_Type,Empty,Obs_Type_Value,Unit_Multiplier,Unit_Of_Measure_Eng,Unit_Of_Measure_Cym,Confidentuality,Empty1,Geographic_Area,Empty2,Empty3,Time_Dim_Item_ID,Time_Dim_Item_Label_Eng,Time_Dim_Item_Label_Cym,Time_Type,Empty4,Statistical_Population_ID,Statistical_Population_Label_Eng,Statistical_Population_Label_Cym,CDID,CDIDDescrip,Empty5,Empty6,Empty7,Empty8,Empty9,Empty10,Empty11,Empty12,Dim_ID_1,dimension_Label_Eng_1,dimension_Label_Cym_1,Dim_Item_ID_1,dimension_Item_Label_Eng_1,dimension_Item_Label_Cym_1,Is_Total_1,Is_Sub_Total_1,Dim_ID_2,dimension_Label_Eng_2,dimension_Label_Cym_2,Dim_Item_ID_2,dimension_Item_Label_Eng_2,dimension_Item_Label_Cym_2,Is_Total_2,Is_Sub_Total_2\n"
var exampleCsvLine string = "153223,,Person,,Count,,,,,,,,,,K04000001,,,,,,,,,,,,,,,,,,,,,Sex,Sex,,All categories: Sex,All categories: Sex,,,,Age,Age,,All categories: Age 16 and over,All categories: Age 16 and over,,,,Residence Type,Residence Type,,All categories: Residence Type,All categories: Residence Type,,,"

func TestProcessor(t *testing.T) {

	kafkaConfig := sarama.NewConfig()
	kafkaConfig.Producer.Retry.Max = 5
	kafkaConfig.Producer.RequiredAcks = sarama.WaitForLocal
	kafkaConfig.Producer.Return.Successes = true

	s3URL := "s3://test-bucket/exampleFilename.csv"
	startTime := time.Now()
	datasetID := "werqae-asdqwrwf-erwe"
	var message *splitter.Message

	Convey("Given a mock producer with a single expected intput that succeeds", t, func() {

		mockProducer := mocks.NewSyncProducer(t, kafkaConfig)
		mockProducer.ExpectSendMessageWithCheckerFunctionAndSucceed(func(val []byte) error {

			//var message *splitter.Message
			json.Unmarshal(val, &message)

			if message.DatasetID != datasetID {
				return errors.New("Dataset ID was not added to the message.")
			}
			if message.S3URL != s3URL {
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
			reader := strings.NewReader(exampleHeaderLine + exampleCsvLine)
			url, _ := url.Parse("s3://bucket/dir/test.csv")

			s3URL := event.NewS3URL(url)
			uploadEvent := &event.FileUploaded{S3URL: s3URL, Time: time.Now().UTC().Unix()}

			Convey("When the processor is called", func() {
				Processor.Process(reader, uploadEvent, startTime, datasetID)

			})
		})
	})

	Convey("Given a mock producer with a single expected intput that fails", t, func() {
		mockProducer := mocks.NewSyncProducer(t, kafkaConfig)
		mockProducer.ExpectSendMessageAndFail(errors.New(""))
		splitter.Producer = mockProducer

		var Processor = splitter.NewCSVProcessor()
		url, _ := url.Parse("s3://bucket/dir/test.csv")

		Convey("Given a reader with a single CSV line", func() {
			reader := strings.NewReader(exampleHeaderLine + exampleCsvLine)

			s3URL := event.NewS3URL(url)
			uploadEvent := &event.FileUploaded{S3URL: s3URL, Time: time.Now().UTC().Unix()}

			Convey("When the processor is called", func() {
				Processor.Process(reader, uploadEvent, startTime, datasetID)
			})
		})
	})
}
