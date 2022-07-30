package storage

import (
	"fmt"
	"os"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/credentials"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"
)

// use `gsutil hmac create [-p <project>] <service_account_email>`
// to get accessKeyID and accessKeySecret for service account
var accessKeyID, accessKeySecret string = initGCPCreds()

func initGCPCreds() (string, string) {
	var key, secret string
	var ok bool

	if key, ok = os.LookupEnv("GCP_ACCESS_KEY_ID"); !ok {
		fmt.Println("MissingCredentials: could not find GCP_ACCESS_KEY_ID")
	}

	if secret, ok = os.LookupEnv("GCP_ACCESS_KEY_SECRET"); !ok {
		fmt.Println("MissingCredentials: could not find GCP_ACCESS_KEY_SECRET")
	}

	return key, secret
}

// OpenGCPConnection .. opens connection to GCP using AWS SDK
func OpenGCPConnection() (*s3.S3, error) {
	sess := session.Must(session.NewSession(&aws.Config{
		Region:      aws.String("auto"),
		Endpoint:    aws.String("https://storage.googleapis.com"),
		Credentials: credentials.NewStaticCredentials(accessKeyID, accessKeySecret, ""),
	}))

	s3 := s3.New(sess)

	return s3, nil
}
