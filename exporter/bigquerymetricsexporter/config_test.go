// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package bigquerymetricsexporter

import (
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/confmap/confmaptest"

	"github.com/open-telemetry/opentelemetry-collector-contrib/exporter/bigquerymetricsexporter/internal/metadata"
)

var _ interface{ Validate() error } = (*Config)(nil)

func TestLoadConfig(t *testing.T) {
	t.Parallel()

	cm, err := confmaptest.LoadConf(filepath.Join("testdata", "config.yaml"))
	require.NoError(t, err)

	tests := []struct {
		id           component.ID
		errorMessage string
	}{
		{
			id: component.NewIDWithName(metadata.Type, ""),
		},
		{
			id: component.NewIDWithName(metadata.Type, "full"),
		},
	}

	for _, tt := range tests {
		t.Run(tt.id.String(), func(t *testing.T) {
			factory := NewFactory()
			cfg := factory.CreateDefaultConfig()

			sub, err := cm.Sub(tt.id.String())
			require.NoError(t, err)
			require.NoError(t, sub.Unmarshal(cfg))

			if tt.errorMessage != "" {
				assert.EqualError(t, cfg.(*Config).Validate(), tt.errorMessage)
				return
			}
			assert.NoError(t, cfg.(*Config).Validate())
		})
	}
}

func TestValidate(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name        string
		cfg         *Config
		errContains string
	}{
		{
			name: "valid minimal config",
			cfg: func() *Config {
				c := createDefaultConfig()
				c.ProjectID = "my-project"
				c.Dataset = "my_dataset"
				return c
			}(),
		},
		{
			name: "missing project_id",
			cfg: func() *Config {
				c := createDefaultConfig()
				c.Dataset = "my_dataset"
				return c
			}(),
			errContains: "project_id is required",
		},
		{
			name: "missing dataset",
			cfg: func() *Config {
				c := createDefaultConfig()
				c.ProjectID = "my-project"
				c.Dataset = ""
				return c
			}(),
			errContains: "dataset is required",
		},
		{
			name: "invalid dataset name",
			cfg: func() *Config {
				c := createDefaultConfig()
				c.ProjectID = "my-project"
				c.Dataset = "123-invalid"
				return c
			}(),
			errContains: "dataset \"123-invalid\" must match pattern",
		},
		{
			name: "invalid metrics_table_name",
			cfg: func() *Config {
				c := createDefaultConfig()
				c.ProjectID = "my-project"
				c.Dataset = "my_dataset"
				c.MetricsTableName = "123-bad"
				return c
			}(),
			errContains: "metrics_table_name \"123-bad\" must match pattern",
		},
		{
			name: "invalid partition_granularity",
			cfg: func() *Config {
				c := createDefaultConfig()
				c.ProjectID = "my-project"
				c.Dataset = "my_dataset"
				c.PartitionGranularity = "WEEK"
				return c
			}(),
			errContains: "partition_granularity must be one of DAY, HOUR, MONTH",
		},
		{
			name: "partition_granularity case insensitive",
			cfg: func() *Config {
				c := createDefaultConfig()
				c.ProjectID = "my-project"
				c.Dataset = "my_dataset"
				c.PartitionGranularity = "hour"
				return c
			}(),
		},
		{
			name: "negative partition_expiration_days",
			cfg: func() *Config {
				c := createDefaultConfig()
				c.ProjectID = "my-project"
				c.Dataset = "my_dataset"
				c.PartitionExpirationDays = -1
				return c
			}(),
			errContains: "partition_expiration_days must be >= 0",
		},
		{
			name: "credentials_file not absolute",
			cfg: func() *Config {
				c := createDefaultConfig()
				c.ProjectID = "my-project"
				c.Dataset = "my_dataset"
				c.CredentialsFile = "relative/path.json"
				return c
			}(),
			errContains: "credentials_file must be an absolute path",
		},
		{
			name: "credentials_file absolute is valid",
			cfg: func() *Config {
				c := createDefaultConfig()
				c.ProjectID = "my-project"
				c.Dataset = "my_dataset"
				c.CredentialsFile = "/absolute/path/creds.json"
				return c
			}(),
		},
		{
			name: "multiple errors",
			cfg: func() *Config {
				c := createDefaultConfig()
				// missing both project_id and dataset
				c.Dataset = ""
				return c
			}(),
			errContains: "project_id is required",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := tt.cfg.Validate()
			if tt.errContains != "" {
				require.Error(t, err)
				assert.Contains(t, err.Error(), tt.errContains)
			} else {
				assert.NoError(t, err)
			}
		})
	}
}

func TestTableName(t *testing.T) {
	cfg := createDefaultConfig()
	cfg.MetricsTableName = "custom_metrics"

	assert.Equal(t, "custom_metrics_gauge", cfg.TableName(gaugeSuffix))
	assert.Equal(t, "custom_metrics_sum", cfg.TableName(sumSuffix))
	assert.Equal(t, "custom_metrics_summary", cfg.TableName(summarySuffix))
	assert.Equal(t, "custom_metrics_histogram", cfg.TableName(histogramSuffix))
	assert.Equal(t, "custom_metrics_exponential_histogram", cfg.TableName(exponentialHistogramSuffix))
}

func TestTableNameDefault(t *testing.T) {
	cfg := createDefaultConfig()

	assert.Equal(t, "otel_metrics_gauge", cfg.TableName(gaugeSuffix))
}
