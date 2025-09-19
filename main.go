package main

import (
	"context"
	"fmt"
	"log"
	"time"

	"github.com/Nnamdichukwu/flow/cmd/config"
	"github.com/Nnamdichukwu/flow/cmd/extract"
	"github.com/google/uuid"
)

func main() {

	if err := config.LoadEnvVars(); err != nil {
		log.Fatal(err)
	}
	fmt.Println("env vars loaded")
	ctx := context.Background()
	pgCredentials := config.PostgresConfig
	if err := config.ConnectPostgresDB(pgCredentials); err != nil {
		log.Fatal(err)

	}
	fmt.Println("connected to postgres")
	sqlDB, err := config.PostgresDB.DB()
	if err != nil {
		log.Fatal(err)
	}

	err = extract.CreateMetadata(ctx)
	if err != nil {
		log.Fatal(err)
	}
	fmt.Println("created metadata table")
	yamlReader, err := extract.ReadYaml("ingest.yaml")
	if err != nil {
		log.Fatal(err)
	}
	log.Println("ingesting metadata")

	yaml := extract.YamlReader{
		SourceTableName:  yamlReader.SourceTableName,
		DestTableName:    yamlReader.DestTableName,
		TimestampColumn:  yamlReader.TimestampColumn,
		WriteDisposition: yamlReader.WriteDisposition,
		InitialLoadDate:  yamlReader.InitialLoadDate,
	}
	log.Printf("writing metadata %s\n", yaml.SourceTableName)
	reader, err := extract.ReadPgTable(ctx, sqlDB, yaml, extract.Metadata{})
	if err != nil {
		log.Fatal(err)
	}
	fmt.Printf("The total size of the table is %d \n", len(reader))

	id := uuid.New().String()
	metadata := extract.Metadata{
		Id:              id,
		SourceTableName: yamlReader.SourceTableName,
		LoadedAt:        time.Now(),
		LoadedBy:        "Postgres",
	}
	err = extract.InsertIntoMetadata(ctx, extract.DuckDB, &metadata)
	if err != nil {
		log.Fatal(err)
	}

	readTable, err := extract.ReadMetadata(ctx, extract.DuckDB, metadata.SourceTableName)
	if err != nil {
		log.Fatal(err)
	}
	fmt.Printf("read metadata successfully for table %s", readTable.LoadedAt)
}
