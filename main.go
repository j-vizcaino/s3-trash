package main

import (
	"context"
	"os"
	"os/signal"
	"sync"
	"time"

	"github.com/aws/aws-sdk-go-v2/config"
	awss3 "github.com/aws/aws-sdk-go-v2/service/s3"
	s3types "github.com/aws/aws-sdk-go-v2/service/s3/types"

	log "github.com/sirupsen/logrus"
	"github.com/spf13/cobra"
)

var (
	awsRegion       string
	connectionCount int
)

type ObjectChannel chan s3types.ObjectIdentifier

func runTrash(cmd *cobra.Command, args []string) {
	bucketName := args[0]
	ctx := cmd.Context()

	awsCfg, err := config.LoadDefaultConfig(context.TODO(), config.WithRegion(awsRegion))
	if err != nil {
		log.WithError(err).Error("Error loading AWS config")
		os.Exit(1)
	}

	s3 := awss3.NewFromConfig(awsCfg)
	status := Status{}
	doneChan := make(chan bool)

	objChan := make(ObjectChannel, MaxBulkOpSize*connectionCount)

	wg := sync.WaitGroup{}

	for i := 0; i < connectionCount; i++ {
		wg.Add(1)
		go DeleteObjects(ctx, bucketName, s3, &status, objChan, &wg)
	}

	go status.Display(time.Second, doneChan)

	if err := listBucket(ctx, bucketName, s3, objChan); err != nil {
		log.WithError(err).Error("Failed to list object versions")
		os.Exit(1)
	}
	close(objChan)
	wg.Wait()

}

func main() {
	log.SetFormatter(&log.TextFormatter{DisableTimestamp: true})

	ctx, _ := signal.NotifyContext(context.Background(), os.Interrupt)
	cmd := &cobra.Command{
		Use:   "s3-trash BUCKET_NAME",
		Short: "Really empty an S3 bucket (including objects' versions)",
		Run:   runTrash,
		Args:  cobra.ExactArgs(1),
	}
	flags := cmd.Flags()
	flags.StringVar(&awsRegion, "region", "us-east-1", "AWS region")
	flags.IntVar(&connectionCount, "connections", 32, "Number of concurrent connections to S3")

	if err := cmd.ExecuteContext(ctx); err != nil {
		os.Exit(1)
	}
}
