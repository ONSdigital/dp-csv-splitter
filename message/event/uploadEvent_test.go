package event

import (
	. "github.com/smartystreets/goconvey/convey"
	"strings"
	"testing"
	"time"
)

const bucketName = "csv-bucket"
const filePath = "/dir1/test-file.csv"

func TestUploadEvent_ConvertToEventDetails(t *testing.T) {

	time := time.Now().UTC().Unix()

	expected := &EventDetails{
		S3URL:      "s3://" + bucketName + filePath,
		BucketName: bucketName,
		FilePath:   strings.TrimPrefix(filePath, "/"),
		Time:       time,
	}

	Convey("Given a valid UploadEvent", t, func() {
		uploadEvent := &UploadEvent{
			S3URL: "s3://" + bucketName + filePath,
			Time:  time,
		}

		Convey("When ConvertToEventDetails is called", func() {
			actual, err := uploadEvent.ConvertToEventDetails()

			Convey("Then the correct value is returned.", func() {
				So(actual.FilePath, ShouldEqual, expected.FilePath)
				So(actual.BucketName, ShouldEqual, expected.BucketName)
				So(actual.S3URL, ShouldEqual, expected.S3URL)
				So(actual.Time, ShouldEqual, expected.Time)
			})

			Convey("And there are no errors", func() {
				So(err, ShouldEqual, nil)
			})
		})
	})

	Convey("Given an UploadEvent with an invalid s3 URL", t, func() {
		uploadEvent := &UploadEvent{
			S3URL: "123456789",
			Time:  time,
		}
		Convey("When ConvertToEventDetails is called", func() {
			_, err := uploadEvent.ConvertToEventDetails()

			Convey("Then the appr opriate error is returned..", func() {
				So(err != nil, ShouldBeTrue)
			})
		})
	})
}
