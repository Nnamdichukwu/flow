package config

import (
	"errors"
	"fmt"
	"os"

	"gorm.io/driver/postgres"
	"gorm.io/gorm"
)

type Postgres struct {
	Host     string
	Port     string
	Password string
	DBName   string
	DBUser   string
}

var PostgresConfig Postgres

func LoadPostgresConfig() error {
	host, exist := os.LookupEnv("DB_HOST")

	if !exist {
		return errors.New("DB_HOST is not set in .env")
	}

	port, exist := os.LookupEnv("DB_PORT")

	if !exist {
		return errors.New("PORT is not set in .env")
	}

	pwd, exist := os.LookupEnv("DB_PASSWORD")

	if !exist {
		return errors.New("DB_PASSWORD is not set in .env")
	}

	dbname, exist := os.LookupEnv("DB_NAME")

	if !exist {
		return errors.New("DB_NAME is not set in .env")
	}

	dbUser, exist := os.LookupEnv("DB_USER")

	if !exist {
		return errors.New("DB_USER is not set in .env")
	}

	PostgresConfig = Postgres{
		Host:     host,
		Port:     port,
		Password: pwd,
		DBName:   dbname,
		DBUser:   dbUser,
	}
	return nil
}

var PostgresDB *gorm.DB

func ConnectPostgresDB(credentials Postgres) error {
	dsn := fmt.Sprintf(
		"host=%s user=%s password=%s dbname=%s port=%s sslmode=disable",
		credentials.Host,
		credentials.DBUser,
		credentials.Password,
		credentials.DBName,
		credentials.Port,
	)

	db, err := gorm.Open(postgres.Open(dsn), &gorm.Config{})

	if err != nil {
		return errors.New("cannot ping db")
	}

	PostgresDB = db

	return nil
}
