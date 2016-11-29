package model

import (
	"encoding/csv"
	"encoding/json"
	"github.com/Shopify/sarama"
	"github.com/Shopify/sarama/mocks"
	. "github.com/smartystreets/goconvey/convey"
	"strings"
	"testing"
)

func TestSpec(t *testing.T) {

	// Only pass t into top-level Convey calls
	Convey("Given some values in a CSV Reader", t, func() {
		records := `record,dimension
		second_record,second_dimension`

		r := csv.NewReader(strings.NewReader(records))

		dataCollectorMock := mocks.NewAsyncProducer(t, nil)
		dataCollectorMock.ExpectInputAndSucceed()

		Convey("I can read individual lines and put it in a queue", func() {
			Loop(r, dataCollectorMock)

			// Expect first message
			expected_msg_json := Message{Index: 0, Row: "record,dimension"}
			j, err := json.Marshal(expected_msg_json)
			m := <-dataCollectorMock.Successes()
			So(err, ShouldBeNil)
			So(m.Value, ShouldEqual, sarama.ByteEncoder(j))

			// Expect second message
			expected_msg_json = Message{Index: 1, Row: "second_record,second_dimension"}
			j, err = json.Marshal(expected_msg_json)
			m = <-dataCollectorMock.Successes()
			So(err, ShouldBeNil)
			So(m.Value, ShouldEqual, sarama.ByteEncoder(j))
		})
	})
}
