package config

import (
	"errors"
	"os"
)

type AwsConfig struct {
	SecretAccessKey string
	AccessKeyID     string
	Region          string
	R2Config        Config
}

var AwsCredentials AwsConfig

func LoadAwsConfig() error {
	secretAccessKey, exist := os.LookupEnv("AWS_SECRET_ACCESS_KEY")
	if !exist {
		return errors.New("AWS_SECRET_ACCESS_KEY environment variable not set")
	}
	accessKey, exist := os.LookupEnv("AWS_ACCESS_KEY_ID")
	if !exist {
		return errors.New("AWS_ACCESS_KEY_ID environment variable not set")
	}
	region, exist := os.LookupEnv("AWS_REGION")
	if !exist {
		return errors.New("AWS_REGION environment variable not set")
	}
	AwsCredentials = AwsConfig{
		SecretAccessKey: secretAccessKey,
		AccessKeyID:     accessKey,
		Region:          region,
		R2Config: Config{
			APIToken:  os.Getenv("R2_API_TOKEN"),
			AccountId: os.Getenv("R2_ACCOUNT_ID"),
		},
	}
	return nil
}
