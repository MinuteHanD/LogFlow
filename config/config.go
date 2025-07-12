package config

import (
	"github.com/spf13/viper"
)

type Config struct {
	Kafka         KafkaConfig
	Elasticsearch ElasticsearchConfig
	Ingestor      IngestorConfig
	Parser        ParserConfig
	StorageWriter StorageWriterConfig `mapstructure:"storage_writer"`
}

type KafkaConfig struct {
	Brokers []string
	Topics  KafkaTopics
}

type KafkaTopics struct {
	Raw       string
	Parsed    string
	RawDLQ    string `mapstructure:"raw_dlq"`
	ParsedDLQ string `mapstructure:"parsed_dlq"`
}

type ElasticsearchConfig struct {
	URL string
}

type IngestorConfig struct {
	HTTPPort int `mapstructure:"http_port"`
}

type ParserConfig struct {
	Version string `mapstructure:"version"`
}

type StorageWriterConfig struct {
	Version     string `mapstructure:"version"`
	IndexPrefix string `mapstructure:"index_prefix"`
}

func LoadConfig() (*Config, error) {
	viper.SetConfigName("config")
	viper.SetConfigType("yaml")
	viper.AddConfigPath(".")

	if err := viper.ReadInConfig(); err != nil {
		return nil, err
	}

	var config Config
	if err := viper.Unmarshal(&config); err != nil {
		return nil, err
	}

	return &config, nil
}
