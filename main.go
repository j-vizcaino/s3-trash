package main

import (
	"os"
	"sync"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	awss3 "github.com/aws/aws-sdk-go/service/s3"
	"github.com/spf13/cobra"
	log "github.com/sirupsen/logrus"
)

var (
	awsRegion string
	connectionCount int
)

type ObjectChannel chan *awss3.ObjectIdentifier

func runTrash(_ *cobra.Command, args []string) {
	bucketName := args[0]

	sess := session.Must(session.NewSession())

	s3 := awss3.New(sess, aws.NewConfig().WithRegion(awsRegion))
	status := Status{}
	doneChan := make(chan bool)

	objChan := make(ObjectChannel, MaxBulkOpSize*connectionCount)

	wg := sync.WaitGroup{}

	for i := 0; i < connectionCount; i++ {
		wg.Add(1)
		go DeleteObjects(bucketName, s3, &status, objChan, &wg)
	}

	go status.Display(time.Second, doneChan)

	err := listBucket(bucketName, s3, objChan)

	if err != nil {
		log.WithError(err).Error("Failed to list object versions")
		os.Exit(1)
	}
	close(objChan)
	wg.Wait()

}

func main() {
	log.SetFormatter(&log.TextFormatter{DisableTimestamp: true})

	cmd := &cobra.Command{
		Use: "s3-trash BUCKET_NAME",
		Short: "Really empty an S3 bucket (including objects' versions)",
		Run: runTrash,
		Args: cobra.ExactArgs(1),
	}
	flags := cmd.Flags()
	flags.StringVar(&awsRegion, "region", "us-east-1", "AWS region")
	flags.IntVar(&connectionCount, "connections", 32, "Number of concurrent connections to S3")

	if err := cmd.Execute(); err != nil {
		os.Exit(1)
	}
}