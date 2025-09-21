package helpers

import (
	"encoding/json"
	"fmt"
	"reflect"
)

func normalizeValue(v interface{}) interface{} {
	switch t := v.(type) {
	case int:
		return int64(t)
	case int32:
		return int64(t)
	case int64:
		return t
	case float32:
		return float64(t)
	case float64:
		return t
	case bool, string:
		return t
	case nil:
		return nil
	default:
		return fmt.Sprintf("%v", t)
	}
}

func NormalizeRow(row map[string]interface{}) map[string]interface{} {
	normalized := map[string]interface{}{}

	for k, v := range row {
		normalized[k] = normalizeValue(v)
	}
	return normalized
}

// GenerateJSONSchema creates a JSON schema from the first row of data
func GenerateJSONSchema(data []map[string]interface{}) (string, error) {
	if len(data) == 0 {
		return "", fmt.Errorf("no data provided to generate schema")
	}
	// Use first row to determine schema
	firstRow := data[0]

	fields := make([]map[string]interface{}, 0, len(firstRow))

	for columnName, value := range firstRow {
		parquetType := getParquetType(value)
		field := map[string]interface{}{
			"Tag": fmt.Sprintf("name=%s, type=%s, convertedtype=UTF8", columnName, parquetType),
		}
		fields = append(fields, field)
	}

	schema := map[string]interface{}{
		"Tag":    "name=parquet_go_root, repetitiontype=REQUIRED",
		"Fields": fields,
	}

	schemaBytes, err := json.Marshal(schema)
	if err != nil {
		return "", err
	}
	return string(schemaBytes), nil
}

// getParquetType maps Go types to Parquet types
func getParquetType(value interface{}) string {
	if value == nil {
		return "BYTE_ARRAY" // Default for null values
	}

	switch reflect.TypeOf(value).Kind() {
	case reflect.Bool:
		return "BOOLEAN"
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32:
		return "INT32"
	case reflect.Int64:
		return "INT64"
	case reflect.Float32:
		return "FLOAT"
	case reflect.Float64:
		return "DOUBLE"
	case reflect.String:
		return "BYTE_ARRAY"
	default:
		return "BYTE_ARRAY" // Default to string for unknown types
	}
}
