package storage

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

type R2Bucket struct {
	BucketName     string
	Key            string
	Body           io.ReadSeeker
	R2Credentials  *config.Config
	AwcCredentials *config.AwsConfig
}

func (r *R2Bucket) Create(ctx context.Context) (r2.Bucket, error) {
	client := cloudflare.NewClient(
		option.WithAPIToken(r.R2Credentials.APIToken))

	bucket, err := client.R2.Buckets.New(ctx, r2.BucketNewParams{
		AccountID: cloudflare.String(r.R2Credentials.AccountId),
		Name:      cloudflare.String(r.BucketName),
	})

	if err != nil {
		return r2.Bucket{}, errors.New("failed to create r2 bucket")
	}
	return *bucket, nil
}

func (r *R2Bucket) GetBucket(ctx context.Context) (r2.Bucket, error) {
	client := cloudflare.NewClient(option.WithAPIToken(r.R2Credentials.APIToken))

	buckets, err := client.R2.Buckets.Get(ctx, r.BucketName, r2.BucketGetParams{
		AccountID: cloudflare.F(r.R2Credentials.AccountId),
	})

	if err != nil {
		return r2.Bucket{}, errors.New("failed to get r2 bucket")
	}
	return *buckets, nil
}

func (r *R2Bucket) UploadObject(ctx context.Context) error {
	sess := session.Must(session.NewSession(&aws.Config{
		Region:      aws.String(r.AwcCredentials.Region),
		Endpoint:    aws.String(fmt.Sprintf("https://%s.r2.cloudflarestorage.com", r.R2Credentials.AccountId)),
		Credentials: credentials.NewStaticCredentials(r.AwcCredentials.AccessKeyID, r.AwcCredentials.SecretAccessKey, ""),
	}))

	uploader := s3manager.NewUploader(sess)

	_, err := uploader.UploadWithContext(ctx, &s3manager.UploadInput{
		Bucket: aws.String(r.BucketName),
		Key:    aws.String(r.Key),
		Body:   r.Body,
	})

	if err != nil {
		return fmt.Errorf("uploading r2 bucket: %w", err)
	}
	return nil
}
