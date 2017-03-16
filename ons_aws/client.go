package ons_aws

import (
	"io"

	"github.com/ONSdigital/dp-csv-splitter/config"
	"github.com/ONSdigital/dp-csv-splitter/message/event"
	"github.com/ONSdigital/go-ns/log"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"
)

// AWSClient interface defining the AWS client.
type AWSService interface {
	GetCSV(event *event.FileUploaded) (io.ReadCloser, error)
}

// Client AWS client implementation.
type Service struct{}

// NewClient create new AWSClient.
func NewService() AWSService {
	return &Service{}
}

// GetFile get the requested file from AWS.
func (cli *Service) GetCSV(event *event.FileUploaded) (io.ReadCloser, error) {
	session, err := session.NewSession(&aws.Config{
		Region: aws.String(config.AWSRegion),
	})

	if err != nil {
		log.Error(err, nil)
		return nil, err
	}

	log.Debug("Requesting .csv file from AWS S3 bucket", log.Data{
		"S3BucketName": event.GetBucketName(),
		"filePath":     event.GetFilePath(),
	})

	s3Service := s3.New(session)
	request := &s3.GetObjectInput{}
	request.SetBucket(event.GetBucketName())
	request.SetKey(event.GetFilePath())

	result, err := s3Service.GetObject(request)

	if err != nil {
		log.Error(err, nil)
		return nil, err
	}

	return result.Body, nil
}
