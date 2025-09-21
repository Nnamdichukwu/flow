package config

import (
	"log"

	"github.com/joho/godotenv"
)

func LoadEnvVars() error {
	_ = godotenv.Load("./.env")
	if err := LoadPostgresConfig(); err != nil {
		log.Println("failed to load postgres config")
		return err
	}
	if err := LoadR2Config(); err != nil {
		log.Println("failed to load r2 config")
		return err
	}

	if err := LoadAwsConfig(); err != nil {
		log.Println("failed to load aws config")
		return err
	}
	return nil

}
