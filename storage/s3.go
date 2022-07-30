package storage

import (
	"bytes"
	"fmt"
	"os"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/awserr"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/pkg/errors"
)

var awsRegion *string = initRegion()

func initRegion() *string {
	var r string
	var ok bool

	// normally the AWS_REGION environment variable should be set
	if r, ok = os.LookupEnv("AWS_REGION"); ok {
		return &r
	}

	return nil
}

// OpenAWS ... opens connection to AWS
func OpenAWS() (*s3.S3, error) {
	s3cfg := aws.NewConfig()
	if awsRegion != nil {
		s3cfg = s3cfg.WithRegion(*awsRegion)
	}

	// open the S3 service
	sess, err := session.NewSession(s3cfg)
	if err != nil {
		return nil, errors.Wrapf(err, "openAWS failed to create s3 session")
	}

	return s3.New(sess), nil
}

// CompleteMultipartUpload .. complete multipart upload for the parts
func CompleteMultipartUpload(client *s3.S3, resp *s3.CreateMultipartUploadOutput, completedParts []*s3.CompletedPart) (*s3.CompleteMultipartUploadOutput, error) {
	completeInput := &s3.CompleteMultipartUploadInput{
		Bucket:   resp.Bucket,
		Key:      resp.Key,
		UploadId: resp.UploadId,
		MultipartUpload: &s3.CompletedMultipartUpload{
			Parts: completedParts,
		},
	}
	return client.CompleteMultipartUpload(completeInput)
}

// UploadPart .. uploads one part of multipart part upload
func UploadPart(client *s3.S3, resp *s3.CreateMultipartUploadOutput, fileBytes []byte, partNumber, maxRetries int) (*s3.CompletedPart, error) {
	tryNum := 1
	partInput := &s3.UploadPartInput{
		Body:          bytes.NewReader(fileBytes),
		Bucket:        resp.Bucket,
		Key:           resp.Key,
		PartNumber:    aws.Int64(int64(partNumber)),
		UploadId:      resp.UploadId,
		ContentLength: aws.Int64(int64(len(fileBytes))),
	}

	for tryNum <= maxRetries {
		uploadResult, err := client.UploadPart(partInput)
		if err != nil {
			if tryNum == maxRetries {
				if aerr, ok := err.(awserr.Error); ok {
					return nil, aerr
				}
				return nil, err
			}
			fmt.Printf("Retrying to upload part #%v\n", partNumber)
			tryNum++
		} else {
			fmt.Printf("Uploaded part #%v\n", partNumber)
			return &s3.CompletedPart{
				ETag:       uploadResult.ETag,
				PartNumber: aws.Int64(int64(partNumber)),
			}, nil
		}
	}
	return nil, nil
}

// AbortMultipartUpload .. aborts multipart upload
func AbortMultipartUpload(client *s3.S3, resp *s3.CreateMultipartUploadOutput) error {
	fmt.Println("Aborting multipart upload for UploadId#" + *resp.UploadId)
	abortInput := &s3.AbortMultipartUploadInput{
		Bucket:   resp.Bucket,
		Key:      resp.Key,
		UploadId: resp.UploadId,
	}
	_, err := client.AbortMultipartUpload(abortInput)
	return err
}
