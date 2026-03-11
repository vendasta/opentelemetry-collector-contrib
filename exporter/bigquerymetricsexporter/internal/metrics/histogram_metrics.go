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

type histogramMetrics struct {
	desc protoreflect.MessageDescriptor
	rows [][]byte
}

// NewHistogramMetrics creates a new MetricsModel for histogram metrics.
func NewHistogramMetrics(desc protoreflect.MessageDescriptor) MetricsModel {
	return &histogramMetrics{desc: desc}
}

func (m *histogramMetrics) Add(resource pcommon.Resource, resourceSchemaURL string, scope pcommon.InstrumentationScope, scopeSchemaURL string, metric pmetric.Metric) {
	common := extractCommonData(resource, resourceSchemaURL, scope, scopeSchemaURL)
	hist := metric.Histogram()

	for i := 0; i < hist.DataPoints().Len(); i++ {
		dp := hist.DataPoints().At(i)

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
		setInt64(msg, m.desc, "aggregation_temporality", int64(hist.AggregationTemporality()))
		setInt64(msg, m.desc, "count", int64(dp.Count()))

		if dp.HasSum() {
			setFloat64(msg, m.desc, "sum", dp.Sum())
		}

		// bucket_counts
		bucketCounts := dp.BucketCounts()
		if bucketCounts.Len() > 0 {
			vals := make([]int64, bucketCounts.Len())
			for j := 0; j < bucketCounts.Len(); j++ {
				vals[j] = int64(bucketCounts.At(j))
			}
			setRepeatedInt64(msg, m.desc, "bucket_counts", vals)
		}

		// explicit_bounds
		bounds := dp.ExplicitBounds()
		if bounds.Len() > 0 {
			vals := make([]float64, bounds.Len())
			for j := 0; j < bounds.Len(); j++ {
				vals[j] = bounds.At(j)
			}
			setRepeatedFloat64(msg, m.desc, "explicit_bounds", vals)
		}

		if dp.HasMin() {
			setFloat64(msg, m.desc, "min", dp.Min())
		}
		if dp.HasMax() {
			setFloat64(msg, m.desc, "max", dp.Max())
		}

		setExemplars(msg, m.desc, dp.Exemplars())

		b, err := serializeRow(msg)
		if err != nil {
			continue
		}
		m.rows = append(m.rows, b)
	}
}

func (m *histogramMetrics) Len() int {
	return len(m.rows)
}

func (m *histogramMetrics) Insert(ctx context.Context, stream *managedwriter.ManagedStream) error {
	if len(m.rows) == 0 {
		return nil
	}
	chunks := chunkRows(m.rows)
	if err := appendWithPipelining(ctx, stream, chunks); err != nil {
		return fmt.Errorf("histogram: %w", err)
	}
	m.rows = nil
	return nil
}
