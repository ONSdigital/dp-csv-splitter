package aws

import (
	"bytes"
	"github.com/ONSdigital/dp-csv-splitter/config"
	"github.com/ONSdigital/go-ns/log"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"
	"io"
	"io/ioutil"
)

// AWSClient interface defining the AWS client.
type AWSService interface {
	GetCSV(filePath string) (io.Reader, error)
}

// Client AWS client implementation.
type Service struct{}

// NewClient create new AWSClient.
func NewService() AWSService {
	return &Service{}
}

// GetFile get the requested file from AWS.
func (cli *Service) GetCSV(filePath string) (io.Reader, error) {
	session, err := session.NewSession(&aws.Config{
		Region: aws.String(config.AWSRegion),
	})

	if err != nil {
		log.Error(err, nil)
		return nil, err
	}

	s3Service := s3.New(session)
	request := &s3.GetObjectInput{}
	request.SetBucket(config.S3Bucket)
	request.SetKey(filePath)

	log.Debug("Requesting .csv file from AWS S3 bucket", log.Data{
		"S3BucketName": config.S3Bucket,
		"filePath": filePath,
	})
	result, err := s3Service.GetObject(request)

	if err != nil {
		log.Error(err, nil)
		return nil, err
	}

	b, err := ioutil.ReadAll(result.Body)
	defer result.Body.Close()

	if err != nil {
		log.Error(err, nil)
		return nil, err
	}

	return bytes.NewReader(b), nil
}
