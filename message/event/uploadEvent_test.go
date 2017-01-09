package event

import (
	. "github.com/smartystreets/goconvey/convey"
	"net/url"
	"strings"
	"testing"
	"time"
)

const bucketName = "csv-bucket"
const filePath = "/dir1/test-file.csv"

var expectedURL, _ = url.Parse("s3://" + bucketName + filePath)

func TestUploadEvent_GetS3BucketName(t *testing.T) {

	Convey("Given an UploadEvent with a valid S3URL.", t, func() {
		event := &UploadEvent{S3URL: "s3://" + bucketName + filePath, Time: time.Now().UTC().Unix()}

		Convey("When GetS3BucketName is called", func() {
			actual := event.GetS3BucketName()

			Convey("Then the correct value is returned.", func() {
				So(actual, ShouldEqual, bucketName)
			})

			Convey("And the url property has been set", func() {
				So(event.url, ShouldNotBeNil)
				So(event.url, ShouldResemble, expectedURL)
			})
		})
	})
}

func TestUploadEvent_GetS3URL(t *testing.T) {
	Convey("Given an UploadEvent with a valid S3URL.", t, func() {
		event := &UploadEvent{S3URL: "s3://" + bucketName + filePath, Time: time.Now().UTC().Unix()}

		Convey("When getS3URL() is called", func() {
			actual := event.GetS3URL()

			Convey("Then the correct value is returned.", func() {
				So(actual, ShouldEqual, expectedURL.String())
			})
		})
	})
}

func TestUploadEvent_GetS3FilePath(t *testing.T) {
	Convey("Given an UploadEvent with a valid S3URL to a file in a sub dir of an s3 bucket .", t, func() {
		event := &UploadEvent{S3URL: "s3://" + bucketName + filePath, Time: time.Now().UTC().Unix()}

		Convey("When GetS3FilePath() is called", func() {
			actual := event.GetS3FilePath()

			Convey("Then the result is the path from the bucket root to the file.", func() {
				So(actual, ShouldEqual, strings.TrimPrefix(filePath, "/"))
			})

			Convey("And there is not leading /.", func() {
				So(actual, ShouldEqual, strings.TrimPrefix(filePath, "/"))
			})
		})
	})
}
