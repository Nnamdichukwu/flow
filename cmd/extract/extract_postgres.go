package extract

import (
	"context"
	"database/sql"
	"fmt"

	_ "github.com/lib/pq"
)

func ReadPgTable(ctx context.Context, db *sql.DB, reader YamlReader, metadata Metadata) ([]map[string]interface{}, error) {
	var (
		query string
		err   error
		rows  *sql.Rows
	)

	loadedAt := metadata.LoadedAt

	if !loadedAt.IsZero() && reader.TimestampColumn != "" {

		query = fmt.Sprintf("SELECT * FROM %s WHERE %s >$1 ", reader.SourceTableName, reader.TimestampColumn)

		rows, err = db.QueryContext(ctx, query, metadata.LoadedAt)

	} else {
		query = fmt.Sprintf("SELECT * FROM %s ", reader.SourceTableName)
		rows, err = db.QueryContext(ctx, query)
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
	if err := rows.Err(); err != nil {
		return nil, err
	}

	return results, nil

}
