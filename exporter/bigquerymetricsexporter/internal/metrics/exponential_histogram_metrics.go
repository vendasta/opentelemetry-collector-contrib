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

type exponentialHistogramMetrics struct {
	desc protoreflect.MessageDescriptor
	rows [][]byte
}

// NewExponentialHistogramMetrics creates a new MetricsModel for exponential histogram metrics.
func NewExponentialHistogramMetrics(desc protoreflect.MessageDescriptor) MetricsModel {
	return &exponentialHistogramMetrics{desc: desc}
}

func (m *exponentialHistogramMetrics) Add(resource pcommon.Resource, resourceSchemaURL string, scope pcommon.InstrumentationScope, scopeSchemaURL string, metric pmetric.Metric) {
	common := extractCommonData(resource, resourceSchemaURL, scope, scopeSchemaURL)
	expHist := metric.ExponentialHistogram()

	for i := 0; i < expHist.DataPoints().Len(); i++ {
		dp := expHist.DataPoints().At(i)

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
		setInt64(msg, m.desc, "aggregation_temporality", int64(expHist.AggregationTemporality()))
		setInt64(msg, m.desc, "count", int64(dp.Count()))

		if dp.HasSum() {
			setFloat64(msg, m.desc, "sum", dp.Sum())
		}

		setInt64(msg, m.desc, "scale", int64(dp.Scale()))
		setInt64(msg, m.desc, "zero_count", int64(dp.ZeroCount()))
		setFloat64(msg, m.desc, "zero_threshold", dp.ZeroThreshold())

		// positive buckets
		positive := dp.Positive()
		setInt64(msg, m.desc, "positive_offset", int64(positive.Offset()))
		if positive.BucketCounts().Len() > 0 {
			vals := make([]int64, positive.BucketCounts().Len())
			for j := 0; j < positive.BucketCounts().Len(); j++ {
				vals[j] = int64(positive.BucketCounts().At(j))
			}
			setRepeatedInt64(msg, m.desc, "positive_bucket_counts", vals)
		}

		// negative buckets
		negative := dp.Negative()
		setInt64(msg, m.desc, "negative_offset", int64(negative.Offset()))
		if negative.BucketCounts().Len() > 0 {
			vals := make([]int64, negative.BucketCounts().Len())
			for j := 0; j < negative.BucketCounts().Len(); j++ {
				vals[j] = int64(negative.BucketCounts().At(j))
			}
			setRepeatedInt64(msg, m.desc, "negative_bucket_counts", vals)
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

func (m *exponentialHistogramMetrics) Len() int {
	return len(m.rows)
}

func (m *exponentialHistogramMetrics) Insert(ctx context.Context, stream *managedwriter.ManagedStream) error {
	if len(m.rows) == 0 {
		return nil
	}
	chunks := chunkRows(m.rows)
	if err := appendWithPipelining(ctx, stream, chunks); err != nil {
		return fmt.Errorf("exponential histogram: %w", err)
	}
	m.rows = nil
	return nil
}
