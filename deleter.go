package main

import (
	"sync"

	"github.com/aws/aws-sdk-go/aws"
	awss3 "github.com/aws/aws-sdk-go/service/s3"
	log "github.com/sirupsen/logrus"
)

const (
	MaxBulkOpSize = 1000
)

func doDelete(s3 *awss3.S3, params *awss3.DeleteObjectsInput, status *Status) {
	_, err := s3.DeleteObjects(params)
	if err != nil {
		if status.IncrementErrors() == 1 {
			log.WithError(err).Error("Failed to delete objects")
		}
	} else {
		lastObject := params.Delete.Objects[len(params.Delete.Objects)-1]
		status.Update(MaxBulkOpSize, *lastObject.Key)
	}
}

func DeleteObjects(bucketName string, s3 *awss3.S3, status *Status, objChan ObjectChannel, wg *sync.WaitGroup) {
	buffer := [MaxBulkOpSize]*awss3.ObjectIdentifier{}
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
			Delete: &awss3.Delete{
				Objects: buffer[:],
			},
		}
		doDelete(s3, &p, status)
	}

	if current != 0 {
		p := awss3.DeleteObjectsInput{
			Bucket: aws.String(bucketName),
			Delete: &awss3.Delete{
				Objects: buffer[:current],
			},
		}
		doDelete(s3, &p, status)
	}
	wg.Done()
}
