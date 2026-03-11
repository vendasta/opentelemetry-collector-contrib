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

type summaryMetrics struct {
	desc protoreflect.MessageDescriptor
	rows [][]byte
}

// NewSummaryMetrics creates a new MetricsModel for summary metrics.
func NewSummaryMetrics(desc protoreflect.MessageDescriptor) MetricsModel {
	return &summaryMetrics{desc: desc}
}

func (m *summaryMetrics) Add(resource pcommon.Resource, resourceSchemaURL string, scope pcommon.InstrumentationScope, scopeSchemaURL string, metric pmetric.Metric) {
	common := extractCommonData(resource, resourceSchemaURL, scope, scopeSchemaURL)
	summary := metric.Summary()

	for i := 0; i < summary.DataPoints().Len(); i++ {
		dp := summary.DataPoints().At(i)

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
		setInt64(msg, m.desc, "flags", int64(dp.Flags()))
		setInt64(msg, m.desc, "count", int64(dp.Count()))
		setFloat64(msg, m.desc, "sum", dp.Sum())
		setQuantileValues(msg, m.desc, dp.QuantileValues())

		b, err := serializeRow(msg)
		if err != nil {
			continue
		}
		m.rows = append(m.rows, b)
	}
}

func (m *summaryMetrics) Len() int {
	return len(m.rows)
}

func (m *summaryMetrics) Insert(ctx context.Context, stream *managedwriter.ManagedStream) error {
	if len(m.rows) == 0 {
		return nil
	}
	chunks := chunkRows(m.rows)
	if err := appendWithPipelining(ctx, stream, chunks); err != nil {
		return fmt.Errorf("summary: %w", err)
	}
	m.rows = nil
	return nil
}
