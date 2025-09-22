package main

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"log"
	"os"
	"sync"
	"time"

	"github.com/Nnamdichukwu/flow/cmd/config"
	"github.com/Nnamdichukwu/flow/cmd/extract"
	"github.com/Nnamdichukwu/flow/cmd/helpers"
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

	ctx, cancel := context.WithCancel(context.Background())

	defer cancel()

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

	log.Println(len(yamlReader.Sources))

	var wg sync.WaitGroup

	sourceChan := make(chan extract.Sources)

	errChan := make(chan error, len(yamlReader.Sources))

	numWorkers := 5

	for i := 0; i < numWorkers; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			for source := range sourceChan {
				if err := processSource(ctx, sqlDB, source); err != nil {
					if errors.Is(err, helpers.ErrNoRows) {
						log.Printf("worker %d: no rows found for %s, skipping\n", id, source.SourceTableName)
						continue

					} else {
						errChan <- err
						cancel()
						return
					}

				}
			}
		}(i + 1)
	}

	for _, source := range yamlReader.Sources {
		sourceChan <- source
	}

	close(sourceChan)

	wg.Wait()

	go func() {
		wg.Wait()
		close(errChan)
	}()

	if err := <-errChan; err != nil {
		log.Fatal(err)
	}

	log.Println("done")
	// Make this work with an R2 data lake
	// Implement pyspark to transform data and load to clickhouse
	// Write unit tests
}

func processSource(ctx context.Context, sqlDB *sql.DB, source extract.Sources) error {
	select {
	case <-ctx.Done():
		return fmt.Errorf("job canceled for processing source: %s", source.SourceTableName)
	default:
	}

	metadata := extract.Metadata{
		Id:              uuid.New().String(),
		SourceTableName: source.SourceTableName,
		LoadedBy:        "Postgres",
	}

	readTable, err := extract.ReadMetadata(ctx, extract.DuckDB, source.SourceTableName)
	fmt.Println(source.SourceTableName)

	if err != nil {
		fmt.Println("failed to read metadata")
		if errors.Is(err, sql.ErrNoRows) {
			readTable = &extract.Metadata{
				Id:              uuid.New().String(),
				SourceTableName: source.SourceTableName,
				LoadedBy:        "Postgres",
				LoadedAt:        time.Now(),
			}
		} else {
			return fmt.Errorf("failed to read metadata: %w", err)
		}
		return fmt.Errorf("failed to read metadata: %w", err)
	}

	fmt.Printf("read metadata successfully for table %s\n", readTable.SourceTableName)

	reader, err := extract.ReadPgTable(ctx, sqlDB, source, *readTable)

	if err != nil {
		return fmt.Errorf("failed to read pg_table from source: %w", err)
	}

	fmt.Printf("read pg_table successfully for table %s\n", readTable.SourceTableName)

	fmt.Printf("length of pg table: %d\n", len(reader))

	if len(reader) == 0 {
		log.Println("No rows found")
		return helpers.ErrNoRows
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
		log.Printf("failed to get bucket: %v", err)
		_, err = r2.Create(ctx)
		if err != nil {
			return fmt.Errorf("failed to create bucket: %w", err)
		}
		fmt.Println("Created bucket")
	}

	fmt.Printf("Bucket already exists %s\n", bucket.Name)

	if len(reader) == 0 {
		return fmt.Errorf("no parquet file to load")
	}

	dir := fmt.Sprintf("./data/%s", metadata.SourceTableName)

	if err = os.MkdirAll(dir, os.ModePerm); err != nil && !os.IsExist(err) {
		return fmt.Errorf("failed to create directory %s: %w", dir, err)
	}

	parquetLoader := loader.ParquetLoader{
		ParquetFile: fmt.Sprintf("%s/%s", dir, time.Now().Format("20060102150405")),
		RowValue:    reader,
	}

	fw, err := local.NewLocalFileWriter(parquetLoader.ParquetFile)

	if err != nil {
		return fmt.Errorf("failed to create local file: %w", err)
	}

	if err = loader.WriteToParquet(fw, reader); err != nil {
		return fmt.Errorf("failed to write parquet file: %w", err)
	}

	parquetFile, err := os.Open(parquetLoader.ParquetFile)

	if err != nil {
		return fmt.Errorf("failed to open parquet file: %w", err)
	}

	log.Printf("parquet loaded successfully")

	r2.Key = fmt.Sprintf("%s/%s.parquet", dir, time.Now().Format("20060102150405"))

	r2.Body = parquetFile

	if err = r2.UploadObject(ctx); err != nil {
		return fmt.Errorf("failed to upload parquet file: %w", err)
	}

	log.Println("uploaded file to r2")

	metadata.LoadedAt = time.Now()

	if err = extract.InsertIntoMetadata(ctx, extract.DuckDB, &metadata); err != nil {
		return fmt.Errorf("failed to insert into metadata: %w", err)
	}

	fmt.Printf("insert metadata successfully for table %s at %s\n", metadata.SourceTableName, metadata.LoadedAt.Format("20060102150405"))

	if err = os.RemoveAll(dir); err != nil {
		return fmt.Errorf("failed to remove directory %s: %w", dir, err)
	}
	return nil
}
