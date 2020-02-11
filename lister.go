package main

import (
	awss3 "github.com/aws/aws-sdk-go/service/s3"
	log "github.com/sirupsen/logrus"
)

func listBucket(bucketName string, s3 *awss3.S3, out ObjectChannel) error {
	params := awss3.ListObjectVersionsInput{}
	params.SetBucket(bucketName)


	log.WithField("bucket", bucketName).Info("Listing object versions in bucket")

	return s3.ListObjectVersionsPages(&params, func(res *awss3.ListObjectVersionsOutput, lastPage bool) bool {
		for _, marker := range res.DeleteMarkers {
			out <- &awss3.ObjectIdentifier{Key: marker.Key, VersionId: marker.VersionId}
		}
		return true
	})
}
