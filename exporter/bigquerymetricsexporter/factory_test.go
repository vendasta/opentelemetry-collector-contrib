// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package bigquerymetricsexporter

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/collector/component/componenttest"
	"go.opentelemetry.io/collector/exporter/exportertest"

	"github.com/open-telemetry/opentelemetry-collector-contrib/exporter/bigquerymetricsexporter/internal/metadata"
)

func TestCreateDefaultConfig(t *testing.T) {
	factory := NewFactory()
	cfg := factory.CreateDefaultConfig()
	assert.NotNil(t, cfg, "failed to create default config")
	assert.NoError(t, componenttest.CheckConfigStruct(cfg))
}

func TestFactory_CreateMetrics(t *testing.T) {
	factory := NewFactory()
	cfg := factory.CreateDefaultConfig().(*Config)
	cfg.ProjectID = "test-project"
	cfg.Dataset = "test_dataset"

	params := exportertest.NewNopSettings(metadata.Type)
	exp, err := factory.CreateMetrics(t.Context(), params, cfg)
	require.NoError(t, err)
	require.NotNil(t, exp)
	require.NoError(t, exp.Shutdown(t.Context()))
}
