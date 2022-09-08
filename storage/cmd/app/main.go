package main

import (
	"flag"
	"fmt"
	"net/http"
	"os"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/manoj-gupta/cloud/storage"
)

const (
	maxPartSize = int64(5 * 1024 * 1024)
	maxRetries  = 3
)

var bucketName string = "mg-aws-bucket"
var provider string = "aws"

func main() {
	flag.StringVar(&bucketName, "bucket", bucketName, "which bucket to access")
	flag.StringVar(&provider, "provider", provider, "which cloud provider (aws, gcp")
	flag.Parse()

	file, err := os.Open("alphabets.txt")
	if err != nil {
		fmt.Printf("err opening file: %s", err)
		return
	}
	defer file.Close()
	fileInfo, _ := file.Stat()
	size := fileInfo.Size()
	buffer := make([]byte, size)
	fileType := http.DetectContentType(buffer)
	file.Read(buffer)

	path := "/multipart/" + file.Name()
	fmt.Printf("Path: %s key: %v filesize:%d\n", path, aws.String(path), size)
	input := &s3.CreateMultipartUploadInput{
		Bucket:      aws.String(bucketName),
		Key:         aws.String(path),
		ContentType: aws.String(fileType),
	}

	var client *s3.S3
	if provider == "gcp" {
		client, err = storage.OpenGCPConnection()
	} else {
		client, err = storage.OpenAWS()
	}
	if err != nil {
		fmt.Println(err.Error())
		return
	}

	resp, err := client.CreateMultipartUpload(input)
	if err != nil {
		fmt.Println(err.Error())
		return
	}
	fmt.Println("Created multipart upload request")

	var curr, partLength int64
	var remaining = size
	var completedParts []*s3.CompletedPart
	partNumber := 1
	for curr = 0; remaining != 0; curr += partLength {
		if remaining < maxPartSize {
			partLength = remaining
		} else {
			partLength = maxPartSize
		}
		fmt.Printf("Uploading part[%d] length[%d]\n", partNumber, partLength)
		completedPart, err := storage.UploadPart(client, resp, buffer[curr:curr+partLength], partNumber, maxRetries)
		if err != nil {
			fmt.Println(err.Error())
			err := storage.AbortMultipartUpload(client, resp)
			if err != nil {
				fmt.Println(err.Error())
			}
			return
		}
		remaining -= partLength
		partNumber++
		completedParts = append(completedParts, completedPart)
	}

	fmt.Printf("Complete Multipart Upload:%v\n", completedParts[2:])
	completeResponse, err := storage.CompleteMultipartUpload(client, resp, completedParts[2:])
	if err != nil {
		fmt.Println(err.Error())
		return
	}

	fmt.Printf("Successfully uploaded file: %s\n", completeResponse.String())
}
