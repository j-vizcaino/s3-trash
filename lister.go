package main

import (
	"context"

	awss3 "github.com/aws/aws-sdk-go-v2/service/s3"
	s3types "github.com/aws/aws-sdk-go-v2/service/s3/types"
	log "github.com/sirupsen/logrus"
)

func listBucket(ctx context.Context, bucketName string, s3 *awss3.Client, out ObjectChannel) error {
	params := awss3.ListObjectVersionsInput{
		Bucket: &bucketName,
	}

	log.WithField("bucket", bucketName).Info("Listing object versions in bucket")

	for {
		res, err := s3.ListObjectVersions(ctx, &params)
		if err != nil {
			return err
		}

		for _, marker := range res.DeleteMarkers {
			out <- s3types.ObjectIdentifier{Key: marker.Key, VersionId: marker.VersionId}
		}
		if res.NextKeyMarker == nil {
			return nil
		}
		params.KeyMarker = res.NextKeyMarker
	}
}
