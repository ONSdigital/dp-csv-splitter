package event

import (
	"net/url"
	"strings"
	"testing"
	"time"

	. "github.com/smartystreets/goconvey/convey"
)

const bucketName = "csv-bucket"
const filePath = "/dir1/test-file.csv"

func TestFileUploaded_GetURL(t *testing.T) {
	s3URL, _ := url.Parse("s3://" + bucketName + filePath)

	Convey("Given a valid FileUploaded", t, func() {

		input := &FileUploaded{
			S3URL: NewS3URL(s3URL),
			Time:  time.Now().UTC().Unix(),
		}

		Convey("When get is called", func() {
			result := input.GetURL()

			Convey("Then the correct value is returned.", func() {
				So(result, ShouldEqual, "s3://"+bucketName+filePath)
			})
		})
	})
}

func TestFileUploaded_GetBucketName(t *testing.T) {
	Convey("Given a valid FileUploaded event.", t, func() {
		s3URL, _ := url.Parse("s3://" + bucketName + filePath)

		input := &FileUploaded{
			S3URL: NewS3URL(s3URL),
			Time:  time.Now().UTC().Unix(),
		}

		Convey("When GetBucketName is called", func() {
			result := input.GetBucketName()

			Convey("Then the correct value is returned.", func() {
				So(result, ShouldEqual, bucketName)
			})
		})
	})
}

func TestFileUploaded_GetFilePath(t *testing.T) {
	Convey("Given a valid FileUploaded event.", t, func() {
		s3URL, _ := url.Parse("s3://" + bucketName + filePath)

		input := &FileUploaded{
			S3URL: NewS3URL(s3URL),
			Time:  time.Now().UTC().Unix(),
		}

		Convey("When GetFilePath is called", func() {
			result := input.GetFilePath()

			Convey("Then the correct value is returned.", func() {
				So(result, ShouldEqual, strings.TrimPrefix(filePath, "/"))
			})
		})
	})
}

func TestS3URLType_UnmarshalJSON(t *testing.T) {
	Convey("Given a valid S3URLType JSON", t, func() {
		s3URL, _ := url.Parse("s3://" + bucketName + filePath)
		expected := NewS3URL(s3URL)

		Convey("When Unmarshalled", func() {
			var actual S3URLType
			err := actual.UnmarshalJSON([]byte("s3://" + bucketName + filePath))

			Convey("Then there are no errors", func() {
				So(err, ShouldBeNil)
			})

			Convey("And the value has been correctly unmarshalled.", func() {
				So(&actual, ShouldResemble, expected)
			})
		})
	})
}
