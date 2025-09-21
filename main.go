package main

import (
	"context"
	"fmt"
	"log"
	"os"
	"time"

	"github.com/Nnamdichukwu/flow/cmd/config"
	"github.com/Nnamdichukwu/flow/cmd/extract"
	"github.com/Nnamdichukwu/flow/cmd/loader"
	"github.com/Nnamdichukwu/flow/cmd/storage"
	"github.com/google/uuid"
	"github.com/xitongsys/parquet-go-source/local"
)

const bucketName = "new-york-fhv"

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

	yaml := extract.YamlReader{
		SourceTableName:  yamlReader.SourceTableName,
		DestTableName:    yamlReader.DestTableName,
		TimestampColumn:  yamlReader.TimestampColumn,
		WriteDisposition: yamlReader.WriteDisposition,
		InitialLoadDate:  yamlReader.InitialLoadDate,
	}

	log.Printf("writing metadata %s\n", yaml.SourceTableName)

	metadata := extract.Metadata{
		Id:              uuid.New().String(),
		SourceTableName: yamlReader.SourceTableName,
		LoadedBy:        "Postgres",
	}

	readTable, err := extract.ReadMetadata(ctx, extract.DuckDB, metadata.SourceTableName)

	if err != nil {
		metadata.LoadedAt = time.Now()
		log.Fatal(err)
	}

	fmt.Printf("read metadata successfully for table %s\n", readTable.SourceTableName)

	reader, err := extract.ReadPgTable(ctx, sqlDB, yaml, *readTable)

	if err != nil {
		log.Fatal(err)
	}

	if len(reader) == 0 {
		log.Fatal("no rows found")
	}

	fmt.Printf("The total size of the table is %d \n", len(reader))

	r2Creds := config.R2Config

	awsCreds := config.AwsCredentials

	r2 := storage.R2Bucket{
		BucketName:     bucketName,
		R2Credentials:  &r2Creds,
		AwcCredentials: &awsCreds,
	}

	bucket, err := r2.GetBucket(ctx)
	if err != nil {
		fmt.Println(err)
		_, err = r2.Create(ctx)
		if err != nil {
			fmt.Println(err)
		}
		fmt.Println("Created bucket")
	}

	fmt.Printf("Bucket already exists %s\n", bucket.Name)

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

	if err = loader.WriteToParquet(fw, reader); err != nil {
		log.Fatal(err)
	}

	parquetFile, err := os.Open(parquetLoader.ParquetFile)

	if err != nil {
		log.Fatal(err)
	}

	log.Printf("parquet loaded successfully")

	r2.Key = fmt.Sprintf("%s/%s.parquet", dir, time.Now().Format("20060102150405"))

	r2.Body = parquetFile

	if err = r2.UploadObject(ctx); err != nil {
		log.Fatal(err)
	}

	log.Println("uploaded file to r2")

	metadata.LoadedAt = time.Now()

	if err = extract.InsertIntoMetadata(ctx, extract.DuckDB, &metadata); err != nil {
		log.Fatal(err)
	}

	fmt.Printf("insert metadata successfully for table %s at %s\n", metadata.SourceTableName, metadata.LoadedAt.Format("20060102150405"))

	if err = os.RemoveAll(dir); err != nil {
		log.Fatal(err)
	}

	log.Println("removed temporary directory")
	// Make this work with an R2 data lake
	// Implement pyspark to transform data and load to clickhouse
	// Write unit tests
}
