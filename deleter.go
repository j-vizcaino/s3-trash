package main

import (
	"context"
	"sync"

	"github.com/aws/aws-sdk-go-v2/aws"
	awss3 "github.com/aws/aws-sdk-go-v2/service/s3"
	s3types "github.com/aws/aws-sdk-go-v2/service/s3/types"
	log "github.com/sirupsen/logrus"
)

const (
	MaxBulkOpSize = 1000
)

func doDelete(ctx context.Context, s3 *awss3.Client, params *awss3.DeleteObjectsInput, status *Status) {
	_, err := s3.DeleteObjects(ctx, params)
	if err != nil {
		if status.IncrementErrors() == 1 {
			log.WithError(err).Error("Failed to delete objects")
		}
	} else {
		lastObject := params.Delete.Objects[len(params.Delete.Objects)-1]
		status.Update(MaxBulkOpSize, *lastObject.Key)
	}
}

func DeleteObjects(ctx context.Context, bucketName string, s3 *awss3.Client, status *Status, objChan ObjectChannel, wg *sync.WaitGroup) {
	buffer := [MaxBulkOpSize]s3types.ObjectIdentifier{}
	current := 0
	for obj := range objChan {

		buffer[current] = obj
		current++
		if current < MaxBulkOpSize {
			continue
		}

		current = 0
		p := awss3.DeleteObjectsInput{
			Bucket: aws.String(bucketName),
			Delete: &s3types.Delete{
				Objects: buffer[:],
			},
		}
		doDelete(ctx, s3, &p, status)
	}

	if current != 0 {
		p := awss3.DeleteObjectsInput{
			Bucket: aws.String(bucketName),
			Delete: &s3types.Delete{
				Objects: buffer[:current],
			},
		}
		doDelete(ctx, s3, &p, status)
	}
	wg.Done()
}
