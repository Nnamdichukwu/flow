package config

import (
	"errors"
	"os"
)

type Config struct {
	APIToken  string
	AccountId string
}

var R2Config Config

func LoadR2Config() error {
	apiToken, exist := os.LookupEnv("R2_API_TOKEN")
	if !exist {
		return errors.New("R2_API_TOKEN environment variable not set")
	}
	accountId, exist := os.LookupEnv("R2_ACCOUNT_ID")
	if !exist {
		return errors.New("R2_ACCOUNT_ID environment variable not set")
	}
	R2Config = Config{
		APIToken:  apiToken,
		AccountId: accountId,
	}
	return nil

}
