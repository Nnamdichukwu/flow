package loader

import (
	"fmt"

	"encoding/json"
	"github.com/Nnamdichukwu/flow/cmd/helpers"
	"github.com/xitongsys/parquet-go/source"
	"github.com/xitongsys/parquet-go/writer"
)

type ParquetLoader struct {
	ParquetFile string
	RowValue    []map[string]interface{}
}

func NormalizeData(data []map[string]interface{}) []map[string]interface{} {
	normalized := make([]map[string]interface{}, len(data))
	for i, row := range data {
		normalized[i] = helpers.NormalizeRow(row)
	}
	return normalized
}

func WriteToParquet(p source.ParquetFile, data []map[string]interface{}) error {
	normalized := NormalizeData(data)

	if len(normalized) == 0 {
		return fmt.Errorf("no data to write")
	}

	// Generate JSON schema dynamically
	jsonSchema, err := helpers.GenerateJSONSchema(normalized)
	if err != nil {
		return fmt.Errorf("failed to generate schema: %v", err)
	}

	// Create JSON writer with dynamic schema
	jw, err := writer.NewJSONWriter(jsonSchema, p, 10)
	if err != nil {
		return fmt.Errorf("failed to create JSON writer: %v", err)
	}
	defer jw.WriteStop()

	// Write each row as JSON
	for _, row := range normalized {
		// Convert map to JSON string
		jsonBytes, err := json.Marshal(row)
		if err != nil {
			return fmt.Errorf("failed to marshal row: %v", err)
		}

		if err := jw.Write(string(jsonBytes)); err != nil {
			return fmt.Errorf("failed to write row: %v", err)
		}
	}

	return nil
}
