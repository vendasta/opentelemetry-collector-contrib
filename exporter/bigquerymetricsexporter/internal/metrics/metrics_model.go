// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package metrics // import "github.com/open-telemetry/opentelemetry-collector-contrib/exporter/bigquerymetricsexporter/internal/metrics"

import (
	"context"
	"encoding/json"
	"fmt"
	"sync"
	"time"

	"cloud.google.com/go/bigquery"
	"cloud.google.com/go/bigquery/storage/managedwriter"
	"cloud.google.com/go/bigquery/storage/managedwriter/adapt"
	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/pmetric"
	"go.uber.org/zap"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protoreflect"
	"google.golang.org/protobuf/types/dynamicpb"
	"google.golang.org/protobuf/types/descriptorpb"
)

const (
	// MaxAppendRowsBytes is the maximum serialized payload per AppendRows call.
	MaxAppendRowsBytes = 10 * 1024 * 1024 // 10MB
	// MaxAppendRowsCount is the maximum number of rows per AppendRows call.
	MaxAppendRowsCount = 50_000

	serviceName = "service.name"
)

// MetricsModel buffers and writes metric data points to BigQuery.
type MetricsModel interface {
	// Add buffers data points from the given metric into the model.
	Add(resource pcommon.Resource, resourceSchemaURL string, scope pcommon.InstrumentationScope, scopeSchemaURL string, metric pmetric.Metric)
	// Len returns the number of buffered rows.
	Len() int
	// Insert writes buffered rows to BigQuery and clears the buffer.
	Insert(ctx context.Context, stream *managedwriter.ManagedStream) error
}

// InsertMetrics dispatches metrics to the correct models and writes concurrently.
func InsertMetrics(ctx context.Context, logger *zap.Logger, md pmetric.Metrics, models map[pmetric.MetricType]MetricsModel, streams map[pmetric.MetricType]*managedwriter.ManagedStream) error {
	// Walk ResourceMetrics → ScopeMetrics → Metrics and dispatch to models.
	for i := 0; i < md.ResourceMetrics().Len(); i++ {
		rm := md.ResourceMetrics().At(i)
		resource := rm.Resource()
		resourceSchemaURL := rm.SchemaUrl()

		for j := 0; j < rm.ScopeMetrics().Len(); j++ {
			sm := rm.ScopeMetrics().At(j)
			scope := sm.Scope()
			scopeSchemaURL := sm.SchemaUrl()

			for k := 0; k < sm.Metrics().Len(); k++ {
				metric := sm.Metrics().At(k)
				mt := metric.Type()

				if mt == pmetric.MetricTypeEmpty {
					logger.Warn("Skipping metric with empty type", zap.String("name", metric.Name()))
					continue
				}

				model, ok := models[mt]
				if !ok {
					logger.Warn("Unsupported metric type", zap.String("type", mt.String()), zap.String("name", metric.Name()))
					continue
				}

				model.Add(resource, resourceSchemaURL, scope, scopeSchemaURL, metric)
			}
		}
	}

	// Fan out inserts concurrently across metric types.
	var (
		wg   sync.WaitGroup
		mu   sync.Mutex
		errs []error
	)

	for mt, model := range models {
		if model.Len() == 0 {
			continue
		}

		stream, ok := streams[mt]
		if !ok {
			continue
		}

		wg.Add(1)
		go func(m MetricsModel, s *managedwriter.ManagedStream, metricType pmetric.MetricType) {
			defer wg.Done()
			if err := m.Insert(ctx, s); err != nil {
				mu.Lock()
				errs = append(errs, fmt.Errorf("insert %s: %w", metricType.String(), err))
				mu.Unlock()
			}
		}(model, stream, mt)
	}

	wg.Wait()

	if len(errs) > 0 {
		return fmt.Errorf("write errors: %v", errs)
	}
	return nil
}

// chunkRows splits serialized rows into chunks respecting size and count limits.
func chunkRows(rows [][]byte) [][][]byte {
	if len(rows) == 0 {
		return nil
	}

	var chunks [][][]byte
	var chunk [][]byte
	var chunkSize int

	for _, row := range rows {
		rowSize := len(row)

		if len(chunk) > 0 && (chunkSize+rowSize > MaxAppendRowsBytes || len(chunk) >= MaxAppendRowsCount) {
			chunks = append(chunks, chunk)
			chunk = nil
			chunkSize = 0
		}

		chunk = append(chunk, row)
		chunkSize += rowSize
	}

	if len(chunk) > 0 {
		chunks = append(chunks, chunk)
	}

	return chunks
}

// appendWithPipelining sends all chunks via AppendRows and then collects all results.
func appendWithPipelining(ctx context.Context, stream *managedwriter.ManagedStream, chunks [][][]byte) error {
	results := make([]*managedwriter.AppendResult, 0, len(chunks))
	for _, chunk := range chunks {
		result, err := stream.AppendRows(ctx, chunk)
		if err != nil {
			return fmt.Errorf("append rows: %w", err)
		}
		results = append(results, result)
	}

	for _, result := range results {
		if _, err := result.GetResult(ctx); err != nil {
			return fmt.Errorf("append result: %w", err)
		}
	}

	return nil
}

// commonRowData holds the shared fields extracted from resource and scope.
type commonRowData struct {
	ResourceSchemaURL    string
	ResourceAttributesJSON []byte
	ScopeName            string
	ScopeVersion         string
	ScopeAttributesJSON  []byte
	ScopeDroppedAttrCount int64
	ScopeSchemaURL       string
	ServiceName          string
}

// extractCommonData pre-serializes shared resource/scope data.
func extractCommonData(resource pcommon.Resource, resourceSchemaURL string, scope pcommon.InstrumentationScope, scopeSchemaURL string) commonRowData {
	svcName := ""
	if v, ok := resource.Attributes().Get(serviceName); ok {
		svcName = v.AsString()
	}

	return commonRowData{
		ResourceSchemaURL:     resourceSchemaURL,
		ResourceAttributesJSON: marshalAttributes(resource.Attributes()),
		ScopeName:             scope.Name(),
		ScopeVersion:          scope.Version(),
		ScopeAttributesJSON:   marshalAttributes(scope.Attributes()),
		ScopeDroppedAttrCount: int64(scope.DroppedAttributesCount()),
		ScopeSchemaURL:        scopeSchemaURL,
		ServiceName:           svcName,
	}
}

// marshalAttributes serializes pcommon.Map to JSON bytes.
func marshalAttributes(attrs pcommon.Map) []byte {
	if attrs.Len() == 0 {
		return nil
	}
	b, _ := json.Marshal(attrs.AsRaw())
	return b
}

// setCommonFields sets the common fields on a dynamic protobuf message.
func setCommonFields(msg *dynamicpb.Message, desc protoreflect.MessageDescriptor, common commonRowData, dp dataPointFields) {
	setString(msg, desc, "resource_schema_url", common.ResourceSchemaURL)
	if common.ResourceAttributesJSON != nil {
		setString(msg, desc, "resource_attributes", string(common.ResourceAttributesJSON))
	}
	setString(msg, desc, "scope_name", common.ScopeName)
	setString(msg, desc, "scope_version", common.ScopeVersion)
	if common.ScopeAttributesJSON != nil {
		setString(msg, desc, "scope_attributes", string(common.ScopeAttributesJSON))
	}
	setInt64(msg, desc, "scope_dropped_attr_count", common.ScopeDroppedAttrCount)
	setString(msg, desc, "scope_schema_url", common.ScopeSchemaURL)
	setString(msg, desc, "service_name", common.ServiceName)
	setString(msg, desc, "metric_name", dp.MetricName)
	setString(msg, desc, "metric_description", dp.MetricDescription)
	setString(msg, desc, "metric_unit", dp.MetricUnit)

	if dp.AttributesJSON != nil {
		setString(msg, desc, "attributes", string(dp.AttributesJSON))
	}

	if dp.StartTimeUnix > 0 {
		setTimestamp(msg, desc, "start_time_unix", dp.StartTimeUnix)
	}
	setTimestamp(msg, desc, "time_unix", dp.TimeUnix)
}

// dataPointFields holds per-data-point fields common to all metric types.
type dataPointFields struct {
	MetricName        string
	MetricDescription string
	MetricUnit        string
	AttributesJSON    []byte
	StartTimeUnix     pcommon.Timestamp
	TimeUnix          pcommon.Timestamp
}

// proto field setter helpers

func setString(msg *dynamicpb.Message, desc protoreflect.MessageDescriptor, name string, value string) {
	if fd := desc.Fields().ByName(protoreflect.Name(name)); fd != nil {
		msg.Set(fd, protoreflect.ValueOfString(value))
	}
}

func setInt64(msg *dynamicpb.Message, desc protoreflect.MessageDescriptor, name string, value int64) {
	if fd := desc.Fields().ByName(protoreflect.Name(name)); fd != nil {
		msg.Set(fd, protoreflect.ValueOfInt64(value))
	}
}

func setFloat64(msg *dynamicpb.Message, desc protoreflect.MessageDescriptor, name string, value float64) {
	if fd := desc.Fields().ByName(protoreflect.Name(name)); fd != nil {
		msg.Set(fd, protoreflect.ValueOfFloat64(value))
	}
}

func setBool(msg *dynamicpb.Message, desc protoreflect.MessageDescriptor, name string, value bool) {
	if fd := desc.Fields().ByName(protoreflect.Name(name)); fd != nil {
		msg.Set(fd, protoreflect.ValueOfBool(value))
	}
}

func setTimestamp(msg *dynamicpb.Message, desc protoreflect.MessageDescriptor, name string, ts pcommon.Timestamp) {
	if fd := desc.Fields().ByName(protoreflect.Name(name)); fd != nil {
		// BigQuery TIMESTAMP is microseconds since epoch.
		usec := ts.AsTime().UnixMicro()
		msg.Set(fd, protoreflect.ValueOfInt64(usec))
	}
}

func serializeRow(msg *dynamicpb.Message) ([]byte, error) {
	return proto.Marshal(msg)
}

// timestampToMicros converts a pcommon.Timestamp to microseconds since epoch.
func timestampToMicros(ts pcommon.Timestamp) int64 {
	return ts.AsTime().UnixMicro()
}

// setRepeatedInt64 sets a repeated int64 field.
func setRepeatedInt64(msg *dynamicpb.Message, desc protoreflect.MessageDescriptor, name string, values []int64) {
	fd := desc.Fields().ByName(protoreflect.Name(name))
	if fd == nil {
		return
	}
	list := msg.Mutable(fd).List()
	for _, v := range values {
		list.Append(protoreflect.ValueOfInt64(v))
	}
}

// setRepeatedFloat64 sets a repeated float64 field.
func setRepeatedFloat64(msg *dynamicpb.Message, desc protoreflect.MessageDescriptor, name string, values []float64) {
	fd := desc.Fields().ByName(protoreflect.Name(name))
	if fd == nil {
		return
	}
	list := msg.Mutable(fd).List()
	for _, v := range values {
		list.Append(protoreflect.ValueOfFloat64(v))
	}
}

// setExemplars encodes exemplar data points as repeated STRUCT messages.
func setExemplars(msg *dynamicpb.Message, desc protoreflect.MessageDescriptor, exemplars pmetric.ExemplarSlice) {
	if exemplars.Len() == 0 {
		return
	}
	fd := desc.Fields().ByName("exemplars")
	if fd == nil {
		return
	}
	list := msg.Mutable(fd).List()
	exemplarDesc := fd.Message()

	for i := 0; i < exemplars.Len(); i++ {
		e := exemplars.At(i)
		eMsg := dynamicpb.NewMessage(exemplarDesc)

		// filtered_attributes
		if e.FilteredAttributes().Len() > 0 {
			attrJSON := marshalAttributes(e.FilteredAttributes())
			setString(eMsg, exemplarDesc, "filtered_attributes", string(attrJSON))
		}

		// time_unix
		if fd := exemplarDesc.Fields().ByName("time_unix"); fd != nil {
			eMsg.Set(fd, protoreflect.ValueOfInt64(e.Timestamp().AsTime().UnixMicro()))
		}

		// value
		var val float64
		switch e.ValueType() {
		case pmetric.ExemplarValueTypeDouble:
			val = e.DoubleValue()
		case pmetric.ExemplarValueTypeInt:
			val = float64(e.IntValue())
		}
		setFloat64(eMsg, exemplarDesc, "value", val)

		// span_id / trace_id
		spanID := e.SpanID()
		if !spanID.IsEmpty() {
			setString(eMsg, exemplarDesc, "span_id", spanID.String())
		}
		traceID := e.TraceID()
		if !traceID.IsEmpty() {
			setString(eMsg, exemplarDesc, "trace_id", traceID.String())
		}

		list.Append(protoreflect.ValueOfMessage(eMsg))
	}
}

// newTimestamp converts a time.Time to pcommon.Timestamp. Exported for testing.
func newTimestamp(t time.Time) pcommon.Timestamp {
	return pcommon.NewTimestampFromTime(t)
}

// SchemaToDescriptor converts a BigQuery schema to a protobuf descriptor
// suitable for use with dynamicpb.NewMessage and managedwriter.
func SchemaToDescriptor(schema bigquery.Schema) (protoreflect.MessageDescriptor, *descriptorpb.DescriptorProto, error) {
	tableSchema, err := adapt.BQSchemaToStorageTableSchema(schema)
	if err != nil {
		return nil, nil, fmt.Errorf("convert BQ schema to storage schema: %w", err)
	}

	descriptor, err := adapt.StorageSchemaToProto2Descriptor(tableSchema, "root")
	if err != nil {
		return nil, nil, fmt.Errorf("convert storage schema to proto descriptor: %w", err)
	}

	messageDescriptor, ok := descriptor.(protoreflect.MessageDescriptor)
	if !ok {
		return nil, nil, fmt.Errorf("descriptor is not a MessageDescriptor")
	}

	dp, err := adapt.NormalizeDescriptor(messageDescriptor)
	if err != nil {
		return nil, nil, fmt.Errorf("normalize descriptor: %w", err)
	}

	return messageDescriptor, dp, nil
}

// getValue extracts a float64 value from a NumberDataPoint.
func getValue(dp pmetric.NumberDataPoint) float64 {
	switch dp.ValueType() {
	case pmetric.NumberDataPointValueTypeDouble:
		return dp.DoubleValue()
	case pmetric.NumberDataPointValueTypeInt:
		return float64(dp.IntValue())
	default:
		return 0
	}
}

// setQuantileValues encodes summary quantile values as repeated STRUCT messages.
func setQuantileValues(msg *dynamicpb.Message, desc protoreflect.MessageDescriptor, qvs pmetric.SummaryDataPointValueAtQuantileSlice) {
	if qvs.Len() == 0 {
		return
	}
	fd := desc.Fields().ByName("quantile_values")
	if fd == nil {
		return
	}
	list := msg.Mutable(fd).List()
	qvDesc := fd.Message()

	for i := 0; i < qvs.Len(); i++ {
		qv := qvs.At(i)
		qvMsg := dynamicpb.NewMessage(qvDesc)
		setFloat64(qvMsg, qvDesc, "quantile", qv.Quantile())
		setFloat64(qvMsg, qvDesc, "value", qv.Value())
		list.Append(protoreflect.ValueOfMessage(qvMsg))
	}
}
