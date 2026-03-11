// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package metrics

import (
	"testing"

	"cloud.google.com/go/bigquery"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestSchemaGauge(t *testing.T) {
	assertCommonFields(t, SchemaGauge)
	assertHasField(t, SchemaGauge, "value", bigquery.FloatFieldType)
	assertHasField(t, SchemaGauge, "flags", bigquery.IntegerFieldType)
	assertHasExemplars(t, SchemaGauge)
}

func TestSchemaSum(t *testing.T) {
	assertCommonFields(t, SchemaSum)
	assertHasField(t, SchemaSum, "value", bigquery.FloatFieldType)
	assertHasField(t, SchemaSum, "flags", bigquery.IntegerFieldType)
	assertHasField(t, SchemaSum, "aggregation_temporality", bigquery.IntegerFieldType)
	assertHasField(t, SchemaSum, "is_monotonic", bigquery.BooleanFieldType)
	assertHasExemplars(t, SchemaSum)
}

func TestSchemaHistogram(t *testing.T) {
	assertCommonFields(t, SchemaHistogram)
	assertHasField(t, SchemaHistogram, "aggregation_temporality", bigquery.IntegerFieldType)
	assertHasField(t, SchemaHistogram, "count", bigquery.IntegerFieldType)
	assertHasField(t, SchemaHistogram, "sum", bigquery.FloatFieldType)
	assertHasRepeatedField(t, SchemaHistogram, "bucket_counts", bigquery.IntegerFieldType)
	assertHasRepeatedField(t, SchemaHistogram, "explicit_bounds", bigquery.FloatFieldType)
	assertHasField(t, SchemaHistogram, "min", bigquery.FloatFieldType)
	assertHasField(t, SchemaHistogram, "max", bigquery.FloatFieldType)
	assertHasExemplars(t, SchemaHistogram)
}

func TestSchemaExponentialHistogram(t *testing.T) {
	assertCommonFields(t, SchemaExponentialHistogram)
	assertHasField(t, SchemaExponentialHistogram, "aggregation_temporality", bigquery.IntegerFieldType)
	assertHasField(t, SchemaExponentialHistogram, "count", bigquery.IntegerFieldType)
	assertHasField(t, SchemaExponentialHistogram, "sum", bigquery.FloatFieldType)
	assertHasField(t, SchemaExponentialHistogram, "scale", bigquery.IntegerFieldType)
	assertHasField(t, SchemaExponentialHistogram, "zero_count", bigquery.IntegerFieldType)
	assertHasField(t, SchemaExponentialHistogram, "zero_threshold", bigquery.FloatFieldType)
	assertHasField(t, SchemaExponentialHistogram, "positive_offset", bigquery.IntegerFieldType)
	assertHasRepeatedField(t, SchemaExponentialHistogram, "positive_bucket_counts", bigquery.IntegerFieldType)
	assertHasField(t, SchemaExponentialHistogram, "negative_offset", bigquery.IntegerFieldType)
	assertHasRepeatedField(t, SchemaExponentialHistogram, "negative_bucket_counts", bigquery.IntegerFieldType)
	assertHasField(t, SchemaExponentialHistogram, "min", bigquery.FloatFieldType)
	assertHasField(t, SchemaExponentialHistogram, "max", bigquery.FloatFieldType)
	assertHasExemplars(t, SchemaExponentialHistogram)
}

func TestSchemaSummary(t *testing.T) {
	assertCommonFields(t, SchemaSummary)
	assertHasField(t, SchemaSummary, "count", bigquery.IntegerFieldType)
	assertHasField(t, SchemaSummary, "sum", bigquery.FloatFieldType)

	qv := findField(SchemaSummary, "quantile_values")
	require.NotNil(t, qv, "missing field quantile_values")
	assert.True(t, qv.Repeated, "quantile_values should be repeated")
	assert.Equal(t, bigquery.RecordFieldType, qv.Type)
	assert.Len(t, qv.Schema, 2)
}

func TestMetricTypeSchemas(t *testing.T) {
	expected := []string{"_gauge", "_sum", "_histogram", "_exponential_histogram", "_summary"}
	for _, suffix := range expected {
		assert.Contains(t, MetricTypeSchemas, suffix)
		assert.NotEmpty(t, MetricTypeSchemas[suffix])
	}
	assert.Len(t, MetricTypeSchemas, len(expected))
}

func TestTimeUnixRequired(t *testing.T) {
	for suffix, schema := range MetricTypeSchemas {
		t.Run(suffix, func(t *testing.T) {
			f := findField(schema, "time_unix")
			require.NotNil(t, f, "missing time_unix field")
			assert.True(t, f.Required, "time_unix should be required")
		})
	}
}

// helpers

func findField(schema bigquery.Schema, name string) *bigquery.FieldSchema {
	for _, f := range schema {
		if f.Name == name {
			return f
		}
	}
	return nil
}

func assertCommonFields(t *testing.T, schema bigquery.Schema) {
	t.Helper()
	commonFieldNames := []string{
		"resource_schema_url", "resource_attributes",
		"scope_name", "scope_version", "scope_attributes",
		"scope_dropped_attr_count", "scope_schema_url",
		"service_name", "metric_name", "metric_description", "metric_unit",
		"attributes", "start_time_unix", "time_unix",
	}
	for _, name := range commonFieldNames {
		assert.NotNilf(t, findField(schema, name), "missing common field %s", name)
	}
}

func assertHasField(t *testing.T, schema bigquery.Schema, name string, ft bigquery.FieldType) {
	t.Helper()
	f := findField(schema, name)
	require.NotNilf(t, f, "missing field %s", name)
	assert.Equal(t, ft, f.Type, "field %s has wrong type", name)
}

func assertHasRepeatedField(t *testing.T, schema bigquery.Schema, name string, ft bigquery.FieldType) {
	t.Helper()
	f := findField(schema, name)
	require.NotNilf(t, f, "missing field %s", name)
	assert.Equal(t, ft, f.Type, "field %s has wrong type", name)
	assert.True(t, f.Repeated, "field %s should be repeated", name)
}

func assertHasExemplars(t *testing.T, schema bigquery.Schema) {
	t.Helper()
	f := findField(schema, "exemplars")
	require.NotNil(t, f, "missing exemplars field")
	assert.True(t, f.Repeated, "exemplars should be repeated")
	assert.Equal(t, bigquery.RecordFieldType, f.Type)
	assert.Len(t, f.Schema, 5, "exemplars should have 5 sub-fields")
}
