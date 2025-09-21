package helpers

import (
	"context"
	"errors"
	"fmt"
	"io"

	"github.com/Nnamdichukwu/flow/cmd/config"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/credentials"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3/s3manager"
	"github.com/cloudflare/cloudflare-go/v3"
	"github.com/cloudflare/cloudflare-go/v3/option"
	"github.com/cloudflare/cloudflare-go/v3/r2"
)

type R2BucketInfo struct {
	BucketName string
	Key        string
	Body       io.ReadSeeker
}

func CreateR2Bucket(ctx context.Context, creds config.Config, bucketName string) (r2.Bucket, error) {
	client := cloudflare.NewClient(
		option.WithAPIToken(creds.APIToken))
	bucket, err := client.R2.Buckets.New(ctx, r2.BucketNewParams{
		AccountID: cloudflare.String(creds.AccountId),
		Name:      cloudflare.String(bucketName),
	})
	if err != nil {
		return r2.Bucket{}, errors.New("failed to create r2 bucket")

	}
	return *bucket, nil

}

func GetBucket(ctx context.Context, creds config.Config, bucketName string) (r2.Bucket, error) {
	client := cloudflare.NewClient(option.WithAPIToken(creds.APIToken))

	buckets, err := client.R2.Buckets.Get(ctx, bucketName, r2.BucketGetParams{
		AccountID: cloudflare.F(creds.AccountId),
	},
	)
	if err != nil {
		return r2.Bucket{}, errors.New("failed to get r2 bucket")
	}
	return *buckets, nil
}

func UploadObject(ctx context.Context, creds config.AwsConfig, r2bucket R2BucketInfo) error {
	sess := session.Must(session.NewSession(&aws.Config{
		Region:      aws.String(creds.Region),
		Endpoint:    aws.String(fmt.Sprintf("https://%s.r2.cloudflarestorage.com", creds.R2Config.AccountId)),
		Credentials: credentials.NewStaticCredentials(creds.AccessKeyID, creds.SecretAccessKey, ""),
	}))
	uploader := s3manager.NewUploader(sess)

	_, err := uploader.UploadWithContext(ctx, &s3manager.UploadInput{
		Bucket: aws.String(r2bucket.BucketName),
		Key:    aws.String(r2bucket.Key),
		Body:   r2bucket.Body,
	})
	if err != nil {
		return fmt.Errorf("uploading r2 bucket: %w", err)
	}
	return nil
}
