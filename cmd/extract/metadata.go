package extract

import (
	"context"
	"database/sql"
	"errors"
	"time"

	_ "github.com/marcboeker/go-duckdb/v2"
)

var DuckDB *sql.DB

type Metadata struct {
	Id              string
	SourceTableName string
	LoadedAt        time.Time
	LoadedBy        string
}

func CreateMetadata(ctx context.Context) error {
	db, err := sql.Open("duckdb", "ingest_metadata.duckdb")
	if err != nil {
		return err
	}

	_, err = db.ExecContext(ctx, `CREATE TABLE IF NOT EXISTS metadata (id VARCHAR, source_table_name VARCHAR, loaded_at TIMESTAMP, loaded_by VARCHAR )`)
	if err != nil {
		return err
	}
	DuckDB = db
	return nil
}

func ReadMetadata(ctx context.Context, db *sql.DB, tableName string) (*Metadata, error) {

	query := `SELECT * FROM metadata WHERE source_table_name = $1`
	row := db.QueryRowContext(ctx, query, tableName)
	var metadata Metadata

	if err := row.Scan(&metadata.Id, &metadata.SourceTableName, &metadata.LoadedAt, &metadata.LoadedBy); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return &Metadata{}, err
		}
	}

	return &metadata, nil

}
func InsertIntoMetadata(ctx context.Context, db *sql.DB, metadata *Metadata) error {
	query := `
        MERGE INTO metadata AS m
        USING (SELECT 
                      $2::VARCHAR AS source_table_name,
                      $3::TIMESTAMP AS loaded_at,
                      $4::VARCHAR AS loaded_by) AS s
        ON m.source_table_name = s.source_table_name
        WHEN MATCHED THEN
            UPDATE SET loaded_at = s.loaded_at
        WHEN NOT MATCHED THEN
            INSERT (id, source_table_name, loaded_at, loaded_by)
            VALUES ($1, s.source_table_name, s.loaded_at, s.loaded_by)
    `
	_, err := db.ExecContext(ctx, query,
		metadata.Id, metadata.SourceTableName, metadata.LoadedAt, metadata.LoadedBy,
	)
	return err
}
