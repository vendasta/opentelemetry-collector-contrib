// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package bigquerymetricsexporter // import "github.com/open-telemetry/opentelemetry-collector-contrib/exporter/bigquerymetricsexporter"

import (
	"errors"
	"fmt"
	"path/filepath"
	"regexp"
	"strings"

	"go.opentelemetry.io/collector/config/configoptional"
	"go.opentelemetry.io/collector/config/configretry"
	"go.opentelemetry.io/collector/exporter/exporterhelper"
)

// Config defines configuration for the BigQuery metrics exporter.
type Config struct {
	exporterhelper.TimeoutConfig  `mapstructure:",squash"`
	configretry.BackOffConfig     `mapstructure:"retry_on_failure"`
	QueueConfig                   configoptional.Optional[exporterhelper.QueueBatchConfig] `mapstructure:"sending_queue"`

	// ProjectID is the GCP project ID for the BigQuery dataset.
	ProjectID string `mapstructure:"project_id"`

	// Dataset is the BigQuery dataset name.
	Dataset string `mapstructure:"dataset"`

	// Location is the BigQuery dataset location. Default: "US".
	Location string `mapstructure:"location"`

	// CreateSchema controls whether the exporter auto-creates the dataset and tables. Default: true.
	CreateSchema bool `mapstructure:"create_schema"`

	// CredentialsFile is an optional path to a GCP credentials JSON file. Overrides ADC.
	CredentialsFile string `mapstructure:"credentials_file"`

	// PartitionGranularity is the time-based partition granularity. Valid values: DAY, HOUR, MONTH. Default: DAY.
	PartitionGranularity string `mapstructure:"partition_granularity"`

	// PartitionExpirationDays is the number of days before partitions expire. 0 means no expiration. Default: 0.
	PartitionExpirationDays int `mapstructure:"partition_expiration_days"`

	// MetricsTableName is the base table name for metrics. Suffixes are auto-appended per metric type. Default: "otel_metrics".
	MetricsTableName string `mapstructure:"metrics_table_name"`
}

const (
	defaultLocation             = "US"
	defaultPartitionGranularity = "DAY"
	defaultMetricsTableName     = "otel_metrics"

	gaugeSuffix                = "_gauge"
	sumSuffix                  = "_sum"
	summarySuffix              = "_summary"
	histogramSuffix            = "_histogram"
	exponentialHistogramSuffix = "_exponential_histogram"
)

var (
	errProjectIDRequired = errors.New("project_id is required")
	errDatasetRequired   = errors.New("dataset is required")

	// identifierPattern validates BigQuery dataset and table names.
	// BigQuery allows up to 1024 chars, starting with letter or underscore.
	identifierPattern = regexp.MustCompile(`^[a-zA-Z_][a-zA-Z0-9_]*$`)

	validPartitionGranularities = map[string]bool{
		"DAY":   true,
		"HOUR":  true,
		"MONTH": true,
	}
)

func createDefaultConfig() *Config {
	return &Config{
		TimeoutConfig:           exporterhelper.NewDefaultTimeoutConfig(),
		BackOffConfig:           configretry.NewDefaultBackOffConfig(),
		QueueConfig:             configoptional.Some(exporterhelper.NewDefaultQueueConfig()),
		Location:                defaultLocation,
		CreateSchema:            true,
		PartitionGranularity:    defaultPartitionGranularity,
		PartitionExpirationDays: 0,
		MetricsTableName:        defaultMetricsTableName,
	}
}

// Validate checks the configuration for errors.
func (cfg *Config) Validate() error {
	var errs []error

	if cfg.ProjectID == "" {
		errs = append(errs, errProjectIDRequired)
	}

	if cfg.Dataset == "" {
		errs = append(errs, errDatasetRequired)
	} else if !identifierPattern.MatchString(cfg.Dataset) {
		errs = append(errs, fmt.Errorf("dataset %q must match pattern %s", cfg.Dataset, identifierPattern.String()))
	}

	if cfg.MetricsTableName != "" && !identifierPattern.MatchString(cfg.MetricsTableName) {
		errs = append(errs, fmt.Errorf("metrics_table_name %q must match pattern %s", cfg.MetricsTableName, identifierPattern.String()))
	}

	granularity := strings.ToUpper(cfg.PartitionGranularity)
	if !validPartitionGranularities[granularity] {
		errs = append(errs, fmt.Errorf("partition_granularity must be one of DAY, HOUR, MONTH; got %q", cfg.PartitionGranularity))
	}
	cfg.PartitionGranularity = granularity

	if cfg.PartitionExpirationDays < 0 {
		errs = append(errs, fmt.Errorf("partition_expiration_days must be >= 0; got %d", cfg.PartitionExpirationDays))
	}

	if cfg.CredentialsFile != "" && !filepath.IsAbs(cfg.CredentialsFile) {
		errs = append(errs, fmt.Errorf("credentials_file must be an absolute path; got %q", cfg.CredentialsFile))
	}

	return errors.Join(errs...)
}

// TableName returns the full table name for a given metric type suffix.
func (cfg *Config) TableName(suffix string) string {
	base := cfg.MetricsTableName
	if base == "" {
		base = defaultMetricsTableName
	}
	return base + suffix
}
