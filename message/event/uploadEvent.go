package event

import (
	"fmt"
	"net/url"
	"strings"
)

const S3_URL_FMT = "s3://%s/%s"

// FileUploaded event
type UploadEvent struct {
	Time  int64  `json:"time"`
	S3URL string `json:"s3URL"`
	url   *url.URL
}

func (f *UploadEvent) GetS3FilePath() string {
	if f.url == nil {
		f.parseURL()
	}
	return strings.TrimPrefix(f.url.Path, "/")
}

func (f *UploadEvent) GetS3BucketName() string {
	if f.url == nil {
		f.parseURL()
	}
	return f.url.Host
}

func (f *UploadEvent) parseURL() {
	f.url, _ = url.Parse(f.S3URL)
}

func (f *UploadEvent) GetS3URL() string {
	return fmt.Sprintf(S3_URL_FMT, f.GetS3BucketName(), f.GetS3FilePath())
}
