package message_test

import (
	"encoding/json"
	"fmt"
	"io"
	"net/url"
	"strings"
	"testing"
	"time"

	"github.com/ONSdigital/dp-csv-splitter/message"
	"github.com/ONSdigital/dp-csv-splitter/message/event"
	"github.com/Shopify/sarama"
	"github.com/Shopify/sarama/mocks"
	. "github.com/smartystreets/goconvey/convey"
)

var messagesProcessed = 0

func TestProcessor(t *testing.T) {
	s3URL, _ := url.Parse("s3://bucket/dir/test.csv")
	event := &event.FileUploaded{
		Time:  time.Now().UTC().Unix(),
		S3URL: event.NewS3URL(s3URL),
	}

	messageJson, _ := json.Marshal(event)
	topicName := "file-uploaded"
	mockConsumer := mocks.NewConsumer(t, nil)
	mockConsumer.ExpectConsumePartition(topicName, 0, 0).YieldMessage(&sarama.ConsumerMessage{Value: []byte(messageJson)})

	mockListener := newMocklistener(mockConsumer, topicName)

	mockProcessor := &mockProcessor{}
	mockAwservice := &mockAwsService{}

	Convey("Given a mock consumer", t, func() {
		messagesProcessed = 0
		go message.ConsumerLoop(mockListener, mockAwservice, mockProcessor)
		loop := 0

		// Give this at least 300 milli-seconds to run before asserting the message was processed
		for loop < 3 {
			if messagesProcessed >= 1 {
				break
			}
			time.Sleep(100 * time.Millisecond)
			loop++
		}
		So(messagesProcessed, ShouldEqual, 1)
		mockConsumer.Close()
	})

}

var exampleHeaderLine string = "Observation,Data_Marking,Statistical_Unit_Eng,Statistical_Unit_Cym,Measure_Type_Eng,Measure_Type_Cym,Observation_Type,Empty,Obs_Type_Value,Unit_Multiplier,Unit_Of_Measure_Eng,Unit_Of_Measure_Cym,Confidentuality,Empty1,Geographic_Area,Empty2,Empty3,Time_Dim_Item_ID,Time_Dim_Item_Label_Eng,Time_Dim_Item_Label_Cym,Time_Type,Empty4,Statistical_Population_ID,Statistical_Population_Label_Eng,Statistical_Population_Label_Cym,CDID,CDIDDescrip,Empty5,Empty6,Empty7,Empty8,Empty9,Empty10,Empty11,Empty12,Dim_ID_1,dimension_Label_Eng_1,dimension_Label_Cym_1,Dim_Item_ID_1,dimension_Item_Label_Eng_1,dimension_Item_Label_Cym_1,Is_Total_1,Is_Sub_Total_1,Dim_ID_2,dimension_Label_Eng_2,dimension_Label_Cym_2,Dim_Item_ID_2,dimension_Item_Label_Eng_2,dimension_Item_Label_Cym_2,Is_Total_2,Is_Sub_Total_2\n"
var exampleCsvLine string = "153223,,Person,,Count,,,,,,,,,,K04000001,,,,,,,,,,,,,,,,,,,,,Sex,Sex,,All categories: Sex,All categories: Sex,,,,Age,Age,,All categories: Age 16 and over,All categories: Age 16 and over,,,,Residence Type,Residence Type,,All categories: Residence Type,All categories: Residence Type,,,"

type mockAwsService struct{}

func (awsService *mockAwsService) GetCSV(event *event.FileUploaded) (io.Reader, error) {
	reader := strings.NewReader(exampleHeaderLine + exampleCsvLine)
	return reader, nil
}

type mockProcessor struct{}

func (processor *mockProcessor) Process(r io.Reader, event *event.FileUploaded, startTime time.Time, datasetID string) {
	messagesProcessed++
	fmt.Println("Processor called!")
}

func newMocklistener(consumer *mocks.Consumer, topic string) mockListener {
	partitionConsumer, _ := consumer.ConsumePartition(topic, 0, 0)
	return mockListener{
		messages: partitionConsumer.Messages(),
	}
}

type mockListener struct {
	message.Listener
	messages <-chan *sarama.ConsumerMessage
}

func (listener mockListener) Messages() <-chan *sarama.ConsumerMessage {
	return listener.messages
}
