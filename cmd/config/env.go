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
	return nil

}
