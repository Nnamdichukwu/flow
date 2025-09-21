package main

import (
	"context"
	"fmt"
	"log"
	"os"
	"time"

	"github.com/Nnamdichukwu/flow/cmd/config"
	"github.com/Nnamdichukwu/flow/cmd/extract"
	"github.com/Nnamdichukwu/flow/cmd/helpers"
	"github.com/Nnamdichukwu/flow/cmd/loader"
	"github.com/google/uuid"
	"github.com/xitongsys/parquet-go-source/local"
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

	reader, err := extract.ReadPgTable(ctx, sqlDB, yaml, metadata)
	if err != nil {
		log.Fatal(err)
	}
	fmt.Printf("The total size of the table is %d \n", len(reader))
	r2Creds := config.R2Config
	bucketName := "new-york-fhv"
	bucket, err := helpers.GetBucket(ctx, r2Creds, bucketName)
	if err != nil {
		fmt.Println(err)
		_, err = helpers.CreateR2Bucket(ctx, r2Creds, bucketName)
	}
	fmt.Printf("created bucket %s\n", bucket.Name)

	if len(reader) == 0 {
		log.Fatal("no parquet file to load")
	}
	dir := fmt.Sprintf("./data/%s", metadata.SourceTableName)
	if err = os.MkdirAll(dir, os.ModePerm); err != nil && !os.IsExist(err) {
		log.Fatal(err)
	}

	parquetLoader := loader.ParquetLoader{
		ParquetFile: fmt.Sprintf("%s/%s", dir, time.Now().Format("20060102150405")),
		RowValue:    reader,
	}
	fw, err := local.NewLocalFileWriter(parquetLoader.ParquetFile)
	if err != nil {
		log.Fatal(err)
	}
	err = loader.WriteToParquet(fw, reader)
	if err != nil {
		log.Fatal(err)
	}
	awsCreds := config.AwsCredentials
	parquetFile, err := os.Open(parquetLoader.ParquetFile)
	if err != nil {
		log.Fatal(err)
	}
	log.Printf("parquet loaded successfully")

	r2BucketInfo := helpers.R2BucketInfo{
		BucketName: bucketName,
		Key:        fmt.Sprintf("%s/%s.parquet", dir, time.Now().Format("20060102150405")),
		Body:       parquetFile,
	}

	err = helpers.UploadObject(ctx, awsCreds, r2BucketInfo)
	log.Println("uploaded file to r2")
	if err != nil {
		log.Fatal(err)
	}
	err = os.RemoveAll(dir)
	if err != nil {
		log.Fatal(err)
	}
	log.Println("removed temporary directory")

}
