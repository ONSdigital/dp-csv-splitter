package event

import (
	"net/url"
	"strings"

	"github.com/ONSdigital/go-ns/log"
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
	if b[0] == '"' && b[len(b)-1] == '"' {
		b = b[1 : len(b)-1]
	}
	bString := string(b)
	url, err := url.Parse(bString)
	if err != nil {
		log.Error(err, log.Data{"Details": "Failed to unmarshal value to S3URLType: " + bString})
		return err
	}
	x.URL = url
	return nil
}

func (x *S3URLType) MarshalJSON() ([]byte, error) {
	urlString := "\"" + x.URL.String() + "\""
	return []byte(urlString), nil
}

func (x *S3URLType) String() string {
	return x.URL.String()
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
