package config

import (
	"github.com/joho/godotenv"
)

func LoadEnvVars() error {
	_ = godotenv.Load("./.env")

	if err := LoadPostgresConfig(); err != nil {
		return err
	}

	if err := LoadR2Config(); err != nil {
		return err
	}

	if err := LoadAwsConfig(); err != nil {
		return err
	}
	return nil
}
