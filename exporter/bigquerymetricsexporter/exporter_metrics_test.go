// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package bigquerymetricsexporter

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap/zaptest"
	"google.golang.org/api/googleapi"
)

func TestNewMetricsExporter(t *testing.T) {
	cfg := createDefaultConfig()
	cfg.ProjectID = "test-project"
	cfg.Dataset = "test_dataset"

	logger := zaptest.NewLogger(t)
	exp := newMetricsExporter(cfg, logger)

	require.NotNil(t, exp)
	assert.Equal(t, cfg, exp.cfg)
	assert.Nil(t, exp.client)
}

func TestPartitionType(t *testing.T) {
	tests := []struct {
		input    string
		expected string
	}{
		{"DAY", "DAY"},
		{"HOUR", "HOUR"},
		{"MONTH", "MONTH"},
		{"unknown", "DAY"}, // defaults to DAY
	}

	for _, tt := range tests {
		t.Run(tt.input, func(t *testing.T) {
			result := partitionType(tt.input)
			assert.Equal(t, tt.expected, string(result))
		})
	}
}

func TestIsAlreadyExists(t *testing.T) {
	tests := []struct {
		name     string
		err      error
		expected bool
	}{
		{
			name:     "409 conflict",
			err:      &googleapi.Error{Code: 409},
			expected: true,
		},
		{
			name:     "404 not found",
			err:      &googleapi.Error{Code: 404},
			expected: false,
		},
		{
			name:     "non-googleapi error",
			err:      assert.AnError,
			expected: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.Equal(t, tt.expected, isAlreadyExists(tt.err))
		})
	}
}

func TestClientOptions_WithCredentials(t *testing.T) {
	cfg := createDefaultConfig()
	cfg.ProjectID = "test-project"
	cfg.Dataset = "test_dataset"
	cfg.CredentialsFile = "/path/to/creds.json"

	logger := zaptest.NewLogger(t)
	exp := newMetricsExporter(cfg, logger)

	opts := exp.clientOptions()
	assert.Len(t, opts, 1)
}

func TestClientOptions_NoCredentials(t *testing.T) {
	cfg := createDefaultConfig()
	cfg.ProjectID = "test-project"
	cfg.Dataset = "test_dataset"

	logger := zaptest.NewLogger(t)
	exp := newMetricsExporter(cfg, logger)

	opts := exp.clientOptions()
	assert.Empty(t, opts)
}

func TestShutdown_NoClient(t *testing.T) {
	cfg := createDefaultConfig()
	cfg.ProjectID = "test-project"
	cfg.Dataset = "test_dataset"

	logger := zaptest.NewLogger(t)
	exp := newMetricsExporter(cfg, logger)

	err := exp.shutdown(t.Context())
	assert.NoError(t, err)
}
