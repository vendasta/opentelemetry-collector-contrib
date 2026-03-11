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

type gaugeMetrics struct {
	desc protoreflect.MessageDescriptor
	rows [][]byte
}

// NewGaugeMetrics creates a new MetricsModel for gauge metrics.
func NewGaugeMetrics(desc protoreflect.MessageDescriptor) MetricsModel {
	return &gaugeMetrics{desc: desc}
}

func (m *gaugeMetrics) Add(resource pcommon.Resource, resourceSchemaURL string, scope pcommon.InstrumentationScope, scopeSchemaURL string, metric pmetric.Metric) {
	common := extractCommonData(resource, resourceSchemaURL, scope, scopeSchemaURL)
	gauge := metric.Gauge()

	for i := 0; i < gauge.DataPoints().Len(); i++ {
		dp := gauge.DataPoints().At(i)

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
		setExemplars(msg, m.desc, dp.Exemplars())

		b, err := serializeRow(msg)
		if err != nil {
			continue
		}
		m.rows = append(m.rows, b)
	}
}

func (m *gaugeMetrics) Len() int {
	return len(m.rows)
}

func (m *gaugeMetrics) Insert(ctx context.Context, stream *managedwriter.ManagedStream) error {
	if len(m.rows) == 0 {
		return nil
	}
	chunks := chunkRows(m.rows)
	if err := appendWithPipelining(ctx, stream, chunks); err != nil {
		return fmt.Errorf("gauge: %w", err)
	}
	m.rows = nil
	return nil
}
