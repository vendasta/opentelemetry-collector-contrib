// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package metrics

import (
	"testing"
	"time"

	"cloud.google.com/go/bigquery/storage/managedwriter"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/pmetric"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protoreflect"
	"google.golang.org/protobuf/types/dynamicpb"
)

func TestSchemaToDescriptor(t *testing.T) {
	for suffix, schema := range MetricTypeSchemas {
		t.Run(suffix, func(t *testing.T) {
			msgDesc, dp, err := SchemaToDescriptor(schema)
			require.NoError(t, err)
			require.NotNil(t, msgDesc)
			require.NotNil(t, dp)

			// Verify common fields exist in the descriptor.
			for _, name := range []string{"resource_schema_url", "scope_name", "metric_name", "time_unix"} {
				fd := msgDesc.Fields().ByName(protoreflect.Name(name))
				assert.NotNilf(t, fd, "missing field %s in %s descriptor", name, suffix)
			}
		})
	}
}

func TestChunkRows(t *testing.T) {
	t.Run("empty", func(t *testing.T) {
		assert.Nil(t, chunkRows(nil))
		assert.Nil(t, chunkRows([][]byte{}))
	})

	t.Run("single chunk", func(t *testing.T) {
		rows := [][]byte{{1}, {2}, {3}}
		chunks := chunkRows(rows)
		require.Len(t, chunks, 1)
		assert.Len(t, chunks[0], 3)
	})

	t.Run("split by count", func(t *testing.T) {
		rows := make([][]byte, MaxAppendRowsCount+1)
		for i := range rows {
			rows[i] = []byte{0}
		}
		chunks := chunkRows(rows)
		require.Len(t, chunks, 2)
		assert.Len(t, chunks[0], MaxAppendRowsCount)
		assert.Len(t, chunks[1], 1)
	})

	t.Run("split by size", func(t *testing.T) {
		bigRow := make([]byte, MaxAppendRowsBytes/2+1)
		rows := [][]byte{bigRow, bigRow}
		chunks := chunkRows(rows)
		require.Len(t, chunks, 2)
		assert.Len(t, chunks[0], 1)
		assert.Len(t, chunks[1], 1)
	})
}

func TestGaugeMetrics_AddAndLen(t *testing.T) {
	desc, _, err := SchemaToDescriptor(SchemaGauge)
	require.NoError(t, err)

	model := NewGaugeMetrics(desc)
	assert.Equal(t, 0, model.Len())

	resource, scope, metric := buildTestGaugeMetric(42.5, time.Now())
	model.Add(resource, "https://schema.url", scope, "https://scope.url", metric)

	assert.Equal(t, 1, model.Len())

	// Verify serialized row can be deserialized.
	gm := model.(*gaugeMetrics)
	require.Len(t, gm.rows, 1)
	msg := dynamicpb.NewMessage(desc)
	require.NoError(t, proto.Unmarshal(gm.rows[0], msg))

	assertProtoField(t, msg, desc, "metric_name", "test.gauge")
	assertProtoField(t, msg, desc, "service_name", "test-service")
	assertProtoFloat64(t, msg, desc, "value", 42.5)
}

func TestSumMetrics_AddAndLen(t *testing.T) {
	desc, _, err := SchemaToDescriptor(SchemaSum)
	require.NoError(t, err)

	model := NewSumMetrics(desc)

	resource, scope, metric := buildTestSumMetric(100.0, true, time.Now())
	model.Add(resource, "", scope, "", metric)

	assert.Equal(t, 1, model.Len())

	sm := model.(*sumMetrics)
	msg := dynamicpb.NewMessage(desc)
	require.NoError(t, proto.Unmarshal(sm.rows[0], msg))

	assertProtoField(t, msg, desc, "metric_name", "test.sum")
	assertProtoFloat64(t, msg, desc, "value", 100.0)
	assertProtoBool(t, msg, desc, "is_monotonic", true)
}

func TestHistogramMetrics_AddAndLen(t *testing.T) {
	desc, _, err := SchemaToDescriptor(SchemaHistogram)
	require.NoError(t, err)

	model := NewHistogramMetrics(desc)

	resource, scope, metric := buildTestHistogramMetric(time.Now())
	model.Add(resource, "", scope, "", metric)

	assert.Equal(t, 1, model.Len())

	hm := model.(*histogramMetrics)
	msg := dynamicpb.NewMessage(desc)
	require.NoError(t, proto.Unmarshal(hm.rows[0], msg))

	assertProtoField(t, msg, desc, "metric_name", "test.histogram")
	assertProtoInt64(t, msg, desc, "count", 10)
}

func TestExponentialHistogramMetrics_AddAndLen(t *testing.T) {
	desc, _, err := SchemaToDescriptor(SchemaExponentialHistogram)
	require.NoError(t, err)

	model := NewExponentialHistogramMetrics(desc)

	resource, scope, metric := buildTestExpHistogramMetric(time.Now())
	model.Add(resource, "", scope, "", metric)

	assert.Equal(t, 1, model.Len())

	ehm := model.(*exponentialHistogramMetrics)
	msg := dynamicpb.NewMessage(desc)
	require.NoError(t, proto.Unmarshal(ehm.rows[0], msg))

	assertProtoField(t, msg, desc, "metric_name", "test.exp_histogram")
	assertProtoInt64(t, msg, desc, "scale", 2)
}

func TestSummaryMetrics_AddAndLen(t *testing.T) {
	desc, _, err := SchemaToDescriptor(SchemaSummary)
	require.NoError(t, err)

	model := NewSummaryMetrics(desc)

	resource, scope, metric := buildTestSummaryMetric(time.Now())
	model.Add(resource, "", scope, "", metric)

	assert.Equal(t, 1, model.Len())

	sm := model.(*summaryMetrics)
	msg := dynamicpb.NewMessage(desc)
	require.NoError(t, proto.Unmarshal(sm.rows[0], msg))

	assertProtoField(t, msg, desc, "metric_name", "test.summary")
	assertProtoInt64(t, msg, desc, "count", 5)
	assertProtoFloat64(t, msg, desc, "sum", 123.45)
}

func TestMultipleDataPoints(t *testing.T) {
	desc, _, err := SchemaToDescriptor(SchemaGauge)
	require.NoError(t, err)

	model := NewGaugeMetrics(desc)

	// Create metric with 3 data points.
	resource := pcommon.NewResource()
	resource.Attributes().PutStr("service.name", "test-service")
	scope := pcommon.NewInstrumentationScope()
	scope.SetName("test-scope")

	md := pmetric.NewMetrics()
	rm := md.ResourceMetrics().AppendEmpty()
	resource.CopyTo(rm.Resource())
	sm := rm.ScopeMetrics().AppendEmpty()
	scope.CopyTo(sm.Scope())
	m := sm.Metrics().AppendEmpty()
	m.SetName("test.multi")
	g := m.SetEmptyGauge()
	for i := 0; i < 3; i++ {
		dp := g.DataPoints().AppendEmpty()
		dp.SetDoubleValue(float64(i))
		dp.SetTimestamp(newTimestamp(time.Now()))
	}

	model.Add(rm.Resource(), "", sm.Scope(), "", m)
	assert.Equal(t, 3, model.Len())
}

func TestInsertMetrics_EmptyModels(t *testing.T) {
	// InsertMetrics with empty metrics should not error.
	md := pmetric.NewMetrics()
	desc, _, err := SchemaToDescriptor(SchemaGauge)
	require.NoError(t, err)

	models := map[pmetric.MetricType]MetricsModel{
		pmetric.MetricTypeGauge: NewGaugeMetrics(desc),
	}
	streams := map[pmetric.MetricType]*managedwriter.ManagedStream{}

	err = InsertMetrics(t.Context(), nil, md, models, streams)
	assert.NoError(t, err)
}

// Test helpers

func buildTestGaugeMetric(value float64, ts time.Time) (pcommon.Resource, pcommon.InstrumentationScope, pmetric.Metric) {
	resource := pcommon.NewResource()
	resource.Attributes().PutStr("service.name", "test-service")
	resource.Attributes().PutStr("host.name", "test-host")

	scope := pcommon.NewInstrumentationScope()
	scope.SetName("test-scope")
	scope.SetVersion("1.0.0")

	md := pmetric.NewMetrics()
	rm := md.ResourceMetrics().AppendEmpty()
	resource.CopyTo(rm.Resource())
	sm := rm.ScopeMetrics().AppendEmpty()
	scope.CopyTo(sm.Scope())
	m := sm.Metrics().AppendEmpty()
	m.SetName("test.gauge")
	m.SetDescription("A test gauge")
	m.SetUnit("bytes")
	g := m.SetEmptyGauge()
	dp := g.DataPoints().AppendEmpty()
	dp.SetDoubleValue(value)
	dp.SetTimestamp(newTimestamp(ts))
	dp.SetStartTimestamp(newTimestamp(ts.Add(-time.Minute)))
	dp.Attributes().PutStr("env", "test")

	return rm.Resource(), sm.Scope(), m
}

func buildTestSumMetric(value float64, monotonic bool, ts time.Time) (pcommon.Resource, pcommon.InstrumentationScope, pmetric.Metric) {
	resource := pcommon.NewResource()
	resource.Attributes().PutStr("service.name", "test-service")

	scope := pcommon.NewInstrumentationScope()
	scope.SetName("test-scope")

	md := pmetric.NewMetrics()
	rm := md.ResourceMetrics().AppendEmpty()
	resource.CopyTo(rm.Resource())
	sm := rm.ScopeMetrics().AppendEmpty()
	scope.CopyTo(sm.Scope())
	m := sm.Metrics().AppendEmpty()
	m.SetName("test.sum")
	s := m.SetEmptySum()
	s.SetIsMonotonic(monotonic)
	s.SetAggregationTemporality(pmetric.AggregationTemporalityCumulative)
	dp := s.DataPoints().AppendEmpty()
	dp.SetDoubleValue(value)
	dp.SetTimestamp(newTimestamp(ts))

	return rm.Resource(), sm.Scope(), m
}

func buildTestHistogramMetric(ts time.Time) (pcommon.Resource, pcommon.InstrumentationScope, pmetric.Metric) {
	resource := pcommon.NewResource()
	resource.Attributes().PutStr("service.name", "test-service")

	scope := pcommon.NewInstrumentationScope()
	scope.SetName("test-scope")

	md := pmetric.NewMetrics()
	rm := md.ResourceMetrics().AppendEmpty()
	resource.CopyTo(rm.Resource())
	sm := rm.ScopeMetrics().AppendEmpty()
	scope.CopyTo(sm.Scope())
	m := sm.Metrics().AppendEmpty()
	m.SetName("test.histogram")
	h := m.SetEmptyHistogram()
	h.SetAggregationTemporality(pmetric.AggregationTemporalityCumulative)
	dp := h.DataPoints().AppendEmpty()
	dp.SetCount(10)
	dp.SetSum(100.0)
	dp.SetMin(1.0)
	dp.SetMax(50.0)
	dp.SetTimestamp(newTimestamp(ts))
	dp.BucketCounts().Append(1, 2, 3, 4)
	dp.ExplicitBounds().Append(10.0, 25.0, 50.0)

	return rm.Resource(), sm.Scope(), m
}

func buildTestExpHistogramMetric(ts time.Time) (pcommon.Resource, pcommon.InstrumentationScope, pmetric.Metric) {
	resource := pcommon.NewResource()
	resource.Attributes().PutStr("service.name", "test-service")

	scope := pcommon.NewInstrumentationScope()
	scope.SetName("test-scope")

	md := pmetric.NewMetrics()
	rm := md.ResourceMetrics().AppendEmpty()
	resource.CopyTo(rm.Resource())
	sm := rm.ScopeMetrics().AppendEmpty()
	scope.CopyTo(sm.Scope())
	m := sm.Metrics().AppendEmpty()
	m.SetName("test.exp_histogram")
	eh := m.SetEmptyExponentialHistogram()
	eh.SetAggregationTemporality(pmetric.AggregationTemporalityDelta)
	dp := eh.DataPoints().AppendEmpty()
	dp.SetCount(20)
	dp.SetSum(200.0)
	dp.SetScale(2)
	dp.SetZeroCount(1)
	dp.SetZeroThreshold(0.001)
	dp.SetTimestamp(newTimestamp(ts))
	dp.Positive().SetOffset(1)
	dp.Positive().BucketCounts().Append(1, 2, 3)
	dp.Negative().SetOffset(-1)
	dp.Negative().BucketCounts().Append(4, 5)

	return rm.Resource(), sm.Scope(), m
}

func buildTestSummaryMetric(ts time.Time) (pcommon.Resource, pcommon.InstrumentationScope, pmetric.Metric) {
	resource := pcommon.NewResource()
	resource.Attributes().PutStr("service.name", "test-service")

	scope := pcommon.NewInstrumentationScope()
	scope.SetName("test-scope")

	md := pmetric.NewMetrics()
	rm := md.ResourceMetrics().AppendEmpty()
	resource.CopyTo(rm.Resource())
	sm := rm.ScopeMetrics().AppendEmpty()
	scope.CopyTo(sm.Scope())
	m := sm.Metrics().AppendEmpty()
	m.SetName("test.summary")
	s := m.SetEmptySummary()
	dp := s.DataPoints().AppendEmpty()
	dp.SetCount(5)
	dp.SetSum(123.45)
	dp.SetTimestamp(newTimestamp(ts))
	qv := dp.QuantileValues().AppendEmpty()
	qv.SetQuantile(0.5)
	qv.SetValue(10.0)
	qv2 := dp.QuantileValues().AppendEmpty()
	qv2.SetQuantile(0.99)
	qv2.SetValue(99.0)

	return rm.Resource(), sm.Scope(), m
}

// Proto assertion helpers

func assertProtoField(t *testing.T, msg *dynamicpb.Message, desc protoreflect.MessageDescriptor, name string, expected string) {
	t.Helper()
	fd := desc.Fields().ByName(protoreflect.Name(name))
	require.NotNilf(t, fd, "missing field %s", name)
	assert.Equal(t, expected, msg.Get(fd).String())
}

func assertProtoFloat64(t *testing.T, msg *dynamicpb.Message, desc protoreflect.MessageDescriptor, name string, expected float64) {
	t.Helper()
	fd := desc.Fields().ByName(protoreflect.Name(name))
	require.NotNilf(t, fd, "missing field %s", name)
	assert.InDelta(t, expected, msg.Get(fd).Float(), 0.001)
}

func assertProtoInt64(t *testing.T, msg *dynamicpb.Message, desc protoreflect.MessageDescriptor, name string, expected int64) {
	t.Helper()
	fd := desc.Fields().ByName(protoreflect.Name(name))
	require.NotNilf(t, fd, "missing field %s", name)
	assert.Equal(t, expected, msg.Get(fd).Int())
}

func assertProtoBool(t *testing.T, msg *dynamicpb.Message, desc protoreflect.MessageDescriptor, name string, expected bool) {
	t.Helper()
	fd := desc.Fields().ByName(protoreflect.Name(name))
	require.NotNilf(t, fd, "missing field %s", name)
	assert.Equal(t, expected, msg.Get(fd).Bool())
}
