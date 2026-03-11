// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package metrics // import "github.com/open-telemetry/opentelemetry-collector-contrib/exporter/bigquerymetricsexporter/internal/metrics"

import (
	"context"
	"fmt"

	"cloud.google.com/go/bigquery/storage/managedwriter"
	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/pmetric"
	"google.golang.org/protobuf/reflect/protoreflect"
	"google.golang.org/protobuf/types/dynamicpb"
)

type sumMetrics struct {
	desc protoreflect.MessageDescriptor
	rows [][]byte
}

// NewSumMetrics creates a new MetricsModel for sum metrics.
func NewSumMetrics(desc protoreflect.MessageDescriptor) MetricsModel {
	return &sumMetrics{desc: desc}
}

func (m *sumMetrics) Add(resource pcommon.Resource, resourceSchemaURL string, scope pcommon.InstrumentationScope, scopeSchemaURL string, metric pmetric.Metric) {
	common := extractCommonData(resource, resourceSchemaURL, scope, scopeSchemaURL)
	sum := metric.Sum()

	for i := 0; i < sum.DataPoints().Len(); i++ {
		dp := sum.DataPoints().At(i)

		dpFields := dataPointFields{
			MetricName:        metric.Name(),
			MetricDescription: metric.Description(),
			MetricUnit:        metric.Unit(),
			AttributesJSON:    marshalAttributes(dp.Attributes()),
			StartTimeUnix:     dp.StartTimestamp(),
			TimeUnix:          dp.Timestamp(),
		}

		msg := dynamicpb.NewMessage(m.desc)
		setCommonFields(msg, m.desc, common, dpFields)
		setFloat64(msg, m.desc, "value", getValue(dp))
		setInt64(msg, m.desc, "flags", int64(dp.Flags()))
		setInt64(msg, m.desc, "aggregation_temporality", int64(sum.AggregationTemporality()))
		setBool(msg, m.desc, "is_monotonic", sum.IsMonotonic())
		setExemplars(msg, m.desc, dp.Exemplars())

		b, err := serializeRow(msg)
		if err != nil {
			continue
		}
		m.rows = append(m.rows, b)
	}
}

func (m *sumMetrics) Len() int {
	return len(m.rows)
}

func (m *sumMetrics) Insert(ctx context.Context, stream *managedwriter.ManagedStream) error {
	if len(m.rows) == 0 {
		return nil
	}
	chunks := chunkRows(m.rows)
	if err := appendWithPipelining(ctx, stream, chunks); err != nil {
		return fmt.Errorf("sum: %w", err)
	}
	m.rows = nil
	return nil
}
