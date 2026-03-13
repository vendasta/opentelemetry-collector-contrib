// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package metrics // import "github.com/open-telemetry/opentelemetry-collector-contrib/exporter/bigquerymetricsexporter/internal/metrics"

import (
	"cloud.google.com/go/bigquery"
)

// Common fields shared by all metric type schemas.
var commonFields = []*bigquery.FieldSchema{
	{Name: "resource_schema_url", Type: bigquery.StringFieldType},
	{Name: "resource_attributes", Type: bigquery.JSONFieldType},
	{Name: "scope_name", Type: bigquery.StringFieldType},
	{Name: "scope_version", Type: bigquery.StringFieldType},
	{Name: "scope_attributes", Type: bigquery.JSONFieldType},
	{Name: "scope_dropped_attr_count", Type: bigquery.IntegerFieldType},
	{Name: "scope_schema_url", Type: bigquery.StringFieldType},
	{Name: "service_name", Type: bigquery.StringFieldType},
	{Name: "metric_name", Type: bigquery.StringFieldType},
	{Name: "metric_description", Type: bigquery.StringFieldType},
	{Name: "metric_unit", Type: bigquery.StringFieldType},
	{Name: "attributes", Type: bigquery.JSONFieldType},
	{Name: "start_time_unix", Type: bigquery.TimestampFieldType},
	{Name: "time_unix", Type: bigquery.TimestampFieldType, Required: true},
}

// exemplarFields defines the schema for exemplars as ARRAY<STRUCT<...>>.
var exemplarFields = &bigquery.FieldSchema{
	Name:     "exemplars",
	Type:     bigquery.RecordFieldType,
	Repeated: true,
	Schema: bigquery.Schema{
		{Name: "filtered_attributes", Type: bigquery.JSONFieldType},
		{Name: "time_unix", Type: bigquery.TimestampFieldType},
		{Name: "value", Type: bigquery.FloatFieldType},
		{Name: "span_id", Type: bigquery.StringFieldType},
		{Name: "trace_id", Type: bigquery.StringFieldType},
	},
}

// SchemaGauge is the BigQuery schema for gauge metrics.
var SchemaGauge = buildSchema(commonFields,
	&bigquery.FieldSchema{Name: "value", Type: bigquery.FloatFieldType},
	&bigquery.FieldSchema{Name: "flags", Type: bigquery.IntegerFieldType},
	exemplarFields,
)

// SchemaSum is the BigQuery schema for sum metrics.
var SchemaSum = buildSchema(commonFields,
	&bigquery.FieldSchema{Name: "value", Type: bigquery.FloatFieldType},
	&bigquery.FieldSchema{Name: "flags", Type: bigquery.IntegerFieldType},
	exemplarFields,
	&bigquery.FieldSchema{Name: "aggregation_temporality", Type: bigquery.IntegerFieldType},
	&bigquery.FieldSchema{Name: "is_monotonic", Type: bigquery.BooleanFieldType},
)

// SchemaHistogram is the BigQuery schema for histogram metrics.
var SchemaHistogram = buildSchema(commonFields,
	&bigquery.FieldSchema{Name: "flags", Type: bigquery.IntegerFieldType},
	exemplarFields,
	&bigquery.FieldSchema{Name: "aggregation_temporality", Type: bigquery.IntegerFieldType},
	&bigquery.FieldSchema{Name: "count", Type: bigquery.IntegerFieldType},
	&bigquery.FieldSchema{Name: "sum", Type: bigquery.FloatFieldType},             // nullable
	&bigquery.FieldSchema{Name: "bucket_counts", Type: bigquery.IntegerFieldType, Repeated: true},
	&bigquery.FieldSchema{Name: "explicit_bounds", Type: bigquery.FloatFieldType, Repeated: true},
	&bigquery.FieldSchema{Name: "min", Type: bigquery.FloatFieldType},             // nullable
	&bigquery.FieldSchema{Name: "max", Type: bigquery.FloatFieldType},             // nullable
)

// SchemaExponentialHistogram is the BigQuery schema for exponential histogram metrics.
var SchemaExponentialHistogram = buildSchema(commonFields,
	&bigquery.FieldSchema{Name: "flags", Type: bigquery.IntegerFieldType},
	exemplarFields,
	&bigquery.FieldSchema{Name: "aggregation_temporality", Type: bigquery.IntegerFieldType},
	&bigquery.FieldSchema{Name: "count", Type: bigquery.IntegerFieldType},
	&bigquery.FieldSchema{Name: "sum", Type: bigquery.FloatFieldType},             // nullable
	&bigquery.FieldSchema{Name: "scale", Type: bigquery.IntegerFieldType},
	&bigquery.FieldSchema{Name: "zero_count", Type: bigquery.IntegerFieldType},
	&bigquery.FieldSchema{Name: "zero_threshold", Type: bigquery.FloatFieldType},
	&bigquery.FieldSchema{Name: "positive_offset", Type: bigquery.IntegerFieldType},
	&bigquery.FieldSchema{Name: "positive_bucket_counts", Type: bigquery.IntegerFieldType, Repeated: true},
	&bigquery.FieldSchema{Name: "negative_offset", Type: bigquery.IntegerFieldType},
	&bigquery.FieldSchema{Name: "negative_bucket_counts", Type: bigquery.IntegerFieldType, Repeated: true},
	&bigquery.FieldSchema{Name: "min", Type: bigquery.FloatFieldType},             // nullable
	&bigquery.FieldSchema{Name: "max", Type: bigquery.FloatFieldType},             // nullable
)

// SchemaSummary is the BigQuery schema for summary metrics.
var SchemaSummary = buildSchema(commonFields,
	&bigquery.FieldSchema{Name: "flags", Type: bigquery.IntegerFieldType},
	&bigquery.FieldSchema{Name: "count", Type: bigquery.IntegerFieldType},
	&bigquery.FieldSchema{Name: "sum", Type: bigquery.FloatFieldType},
	&bigquery.FieldSchema{
		Name:     "quantile_values",
		Type:     bigquery.RecordFieldType,
		Repeated: true,
		Schema: bigquery.Schema{
			{Name: "quantile", Type: bigquery.FloatFieldType},
			{Name: "value", Type: bigquery.FloatFieldType},
		},
	},
)

// MetricTypeSchemas maps metric type suffixes to their BigQuery schemas.
var MetricTypeSchemas = map[string]bigquery.Schema{
	"_gauge":                    SchemaGauge,
	"_sum":                      SchemaSum,
	"_histogram":                SchemaHistogram,
	"_exponential_histogram":    SchemaExponentialHistogram,
	"_summary":                  SchemaSummary,
}

// compactExcludedFields lists fields excluded in compact verbosity mode.
var compactExcludedFields = map[string]bool{
	"resource_schema_url":      true,
	"metric_description":       true,
	"scope_name":               true,
	"scope_version":            true,
	"scope_attributes":         true,
	"scope_dropped_attr_count": true,
	"scope_schema_url":         true,
	"exemplars":                true,
}

// BuildSchemas returns metric type schemas filtered by the given verbosity level.
// "full" returns all fields (the shared MetricTypeSchemas — callers must not mutate).
// "compact" excludes exemplars, metric_description, resource_schema_url, and all scope_* fields.
func BuildSchemas(verbosity string) map[string]bigquery.Schema {
	if verbosity != "compact" {
		return MetricTypeSchemas
	}

	result := make(map[string]bigquery.Schema, len(MetricTypeSchemas))
	for suffix, schema := range MetricTypeSchemas {
		filtered := make(bigquery.Schema, 0, len(schema))
		for _, f := range schema {
			if !compactExcludedFields[f.Name] {
				filtered = append(filtered, f)
			}
		}
		result[suffix] = filtered
	}
	return result
}

// buildSchema constructs a schema by concatenating common fields with type-specific fields.
func buildSchema(common []*bigquery.FieldSchema, extra ...*bigquery.FieldSchema) bigquery.Schema {
	schema := make(bigquery.Schema, 0, len(common)+len(extra))
	schema = append(schema, common...)
	schema = append(schema, extra...)
	return schema
}
