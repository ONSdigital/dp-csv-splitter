package event

import (
	"github.com/ONSdigital/go-ns/log"
	"net/url"
	"strings"
)

// FileUploaded event
type FileUploaded struct {
	Time  int64
	S3URL *S3URLType
}

type S3URLType struct {
	URL *url.URL
}

func NewS3URL(s3url *url.URL) *S3URLType {
	return &S3URLType{s3url}
}

func (x *S3URLType) UnmarshalJSON(b []byte) (err error) {
	if b[0] == '"' && b[len(b) - 1] == '"' {
		b = b[1 : len(b) - 1]
	}
	url, err := url.Parse(string(b))
	if err != nil {
		log.Error(err, log.Data{"Details": "Failed to unmarshal value to S3URLType"})
		return err
	}
	x.URL = url
	return nil
}

func (d *FileUploaded) GetBucketName() string {
	return d.S3URL.URL.Host
}

func (d *FileUploaded) GetFilePath() string {
	return strings.TrimPrefix(d.S3URL.URL.Path, "/")
}

func (d *FileUploaded) GetURL() string {
	return d.S3URL.URL.String()
}
