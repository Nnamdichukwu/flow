package extract

import (
	"context"
	"database/sql"
	"fmt"

	"github.com/Nnamdichukwu/flow/cmd/helpers"
	_ "github.com/lib/pq"
)

func ReadPgTable(ctx context.Context, db *sql.DB, source Sources, metadata Metadata) ([]map[string]interface{}, error) {
	var (
		query string
		err   error
		rows  *sql.Rows
	)

	loadedAt := metadata.LoadedAt

	fmt.Printf("Loaded at %v\n", loadedAt)

	if !loadedAt.IsZero() && source.TimestampColumn != "" {
		query = fmt.Sprintf("SELECT * FROM %s WHERE %s >=$1 ", source.SourceTableName, source.TimestampColumn)
		fmt.Printf("Query: %s\n", query)
		rows, err = db.QueryContext(ctx, query, metadata.LoadedAt.UTC())
	} else if loadedAt.IsZero() && source.InitialLoadDate != nil && source.TimestampColumn != "" {
		query = fmt.Sprintf("SELECT * FROM %s WHERE %s >=$1 ", source.SourceTableName, source.TimestampColumn)
		rows, err = db.QueryContext(ctx, query, source.InitialLoadDate.UTC())
		fmt.Printf("Query: %s\n", query)
	} else {
		query = fmt.Sprintf("SELECT * FROM %s ", source.SourceTableName)
		rows, err = db.QueryContext(ctx, query)
		fmt.Printf("Query: %s\n", query)
	}

	if err != nil {
		return nil, err
	}

	defer rows.Close()

	columns, err := rows.Columns()

	if err != nil {
		return nil, err
	}

	values := make([]interface{}, len(columns))

	scanArgs := make([]interface{}, len(columns))

	for i := range values {
		scanArgs[i] = &values[i]
	}

	var results []map[string]interface{}

	for rows.Next() {
		err = rows.Scan(scanArgs...)
		if err != nil {
			return nil, err
		}
		rowValues := make(map[string]interface{}, len(columns))
		for i, col := range columns {
			val := values[i]
			b, ok := val.([]byte)
			if ok {
				rowValues[col] = string(b)
			} else {
				rowValues[col] = val
			}
		}
		results = append(results, rowValues)
	}
	if len(results) == 0 {
		return nil, helpers.ErrNoRows
	}
	return results, nil
}
