package event

import (
	"encoding/json"
	"net/url"
	"strings"
)

type UploadEvent struct {
	Time  int64  `json:"time"`
	S3URL string `json:"s3URL"`
}

func (event *UploadEvent) ConvertToEventDetails() (*EventDetails, error) {
	s3url, err := url.Parse(event.S3URL)
	if err != nil {
		return nil, err
	}
	_, err = url.ParseRequestURI(s3url.RequestURI())
	if err != nil {
		return nil, err
	}
	return &EventDetails{
		Time:       event.Time,
		FilePath:   strings.TrimPrefix(s3url.Path, "/"),
		BucketName: s3url.Host,
		S3URL:      s3url.String(),
	}, nil
}

// FileUploaded event
type EventDetails struct {
	Time       int64
	FilePath   string
	BucketName string
	S3URL      string
}

func (e *EventDetails) String() string {
	json, _ := json.Marshal(e)
	return string(json)
}
