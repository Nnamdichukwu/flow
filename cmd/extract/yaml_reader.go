package extract

import (
	"errors"
	"os"
	"time"

	"gopkg.in/yaml.v3"
)

type YamlReader struct {
	SourceTableName  string     `yaml:"source_table_name"`
	DestTableName    string     `yaml:"dest_table_name"`
	TimestampColumn  string     `yaml:"timestamp_column"`
	WriteDisposition string     `yaml:"write_disposition"`
	InitialLoadDate  *time.Time `yaml:"initial_load_date"`
}

func ReadYaml(file string) (*YamlReader, error) {
	data, err := os.ReadFile(file)

	if err != nil {
		return nil, errors.New("failed to read yaml file")
	}

	var yamlReader YamlReader
	err = yaml.Unmarshal(data, &yamlReader)
	if err != nil {
		return nil, err
	}
	return &yamlReader, nil

}
