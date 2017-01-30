package splitter_test

import (
	"encoding/json"
	"errors"
	"net/url"
	"strings"
	"testing"
	"time"

	"github.com/ONSdigital/dp-csv-splitter/message/event"
	"github.com/ONSdigital/dp-csv-splitter/splitter"
	"github.com/Shopify/sarama"
	. "github.com/smartystreets/goconvey/convey"
	"github.com/ONSdigital/dp-csv-splitter/config"
)

var exampleHeaderLine string = "Observation,Data_Marking,Statistical_Unit_Eng,Statistical_Unit_Cym,Measure_Type_Eng,Measure_Type_Cym,Observation_Type,Empty,Obs_Type_Value,Unit_Multiplier,Unit_Of_Measure_Eng,Unit_Of_Measure_Cym,Confidentuality,Empty1,Geographic_Area,Empty2,Empty3,Time_Dim_Item_ID,Time_Dim_Item_Label_Eng,Time_Dim_Item_Label_Cym,Time_Type,Empty4,Statistical_Population_ID,Statistical_Population_Label_Eng,Statistical_Population_Label_Cym,CDID,CDIDDescrip,Empty5,Empty6,Empty7,Empty8,Empty9,Empty10,Empty11,Empty12,Dim_ID_1,dimension_Label_Eng_1,dimension_Label_Cym_1,Dim_Item_ID_1,dimension_Item_Label_Eng_1,dimension_Item_Label_Cym_1,Is_Total_1,Is_Sub_Total_1,Dim_ID_2,dimension_Label_Eng_2,dimension_Label_Cym_2,Dim_Item_ID_2,dimension_Item_Label_Eng_2,dimension_Item_Label_Cym_2,Is_Total_2,Is_Sub_Total_2\n"
var exampleCsvLine string = "153223,,Person,,Count,,,,,,,,,,K04000001,,,,,,,,,,,,,,,,,,,,,Sex,Sex,,All categories: Sex,All categories: Sex,,,,Age,Age,,All categories: Age 16 and over,All categories: Age 16 and over,,,,Residence Type,Residence Type,,All categories: Residence Type,All categories: Residence Type,,,"


type MockProducer struct {
	singleMessageInvocations    []*sarama.ProducerMessage
	multipleMessagesInvocations [][]*sarama.ProducerMessage
	throwError                  bool
}

func (mock *MockProducer) SendMessage(msg *sarama.ProducerMessage) (partition int32, offset int64, err error) {
	mock.singleMessageInvocations = append(mock.singleMessageInvocations, msg)
	if mock.throwError {
		return 0, 0, errors.New("Mock error sending message")
	}
	return 0, 0, nil
}

func (mock *MockProducer) SendMessages(msgs []*sarama.ProducerMessage) error {
	mock.multipleMessagesInvocations = append(mock.multipleMessagesInvocations, msgs)
	if mock.throwError {
		return errors.New("Mock error sending messages")
	}
	return nil
}

func (mock *MockProducer) Close() error {
	return nil
}

func TestProcess(t *testing.T) {

	startTime := time.Now()
	datasetID := "werqae-asdqwrwf-erwe"
	reader := strings.NewReader(exampleHeaderLine + exampleCsvLine + "\n" + exampleCsvLine)
	url, _ := url.Parse("s3://bucket/dir/test.csv")

	s3URL := event.NewS3URL(url)
	uploadEvent := &event.FileUploaded{S3URL: s3URL, Time: time.Now().UTC().Unix()}

	mockProducer := &MockProducer{}

	Convey("Given a mock mockProducer with two rows that succeeds", t, func() {
		splitter.Producer = mockProducer

		var processor = splitter.NewCSVProcessor()

		Convey("When the processor is called", func() {
			processor.Process(reader, uploadEvent, startTime, datasetID)

			So(len(mockProducer.multipleMessagesInvocations), ShouldEqual, 1)
			So(len(mockProducer.multipleMessagesInvocations[0]), ShouldEqual, 2)
			for i := 0; i < 2; i++ {
				producerMessage := mockProducer.multipleMessagesInvocations[0][i]
				So(producerMessage.Topic, ShouldEqual, config.RowTopicName)
				rowMessage := extractRowMessage(producerMessage)

				So(rowMessage.DatasetID, ShouldEqual, datasetID)
				So(rowMessage.S3URL, ShouldEqual, url.String())
				So(rowMessage.StartTime, ShouldEqual, startTime.UTC().Unix())
				So(rowMessage.Index, ShouldEqual, i)
				So(rowMessage.Row, ShouldEqual, exampleCsvLine)
			}

			So(len(mockProducer.singleMessageInvocations), ShouldEqual, 1)
			producerMessage := mockProducer.singleMessageInvocations[0]
			So(producerMessage.Topic, ShouldEqual, config.DatasetTopicName)
			datasetMessage := extractDatasetMessage(mockProducer.singleMessageInvocations[0])
			So(datasetMessage.DatasetID, ShouldEqual, datasetID)
			So(datasetMessage.TotalRows, ShouldEqual, 2)
		})

	})


}

func extractRowMessage(producerMessage *sarama.ProducerMessage) *splitter.RowMessage {
	var message *splitter.RowMessage
	val, _ := producerMessage.Value.Encode()
	json.Unmarshal(val, &message)

	return message
}
func extractDatasetMessage(producerMessage *sarama.ProducerMessage) *splitter.DatasetSplitEvent {
	var message *splitter.DatasetSplitEvent
	val, _ := producerMessage.Value.Encode()
	json.Unmarshal(val, &message)

	return message
}
