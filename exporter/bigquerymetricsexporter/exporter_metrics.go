// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package bigquerymetricsexporter // import "github.com/open-telemetry/opentelemetry-collector-contrib/exporter/bigquerymetricsexporter"

import (
	"context"
	"fmt"
	"time"

	"cloud.google.com/go/bigquery"
	"cloud.google.com/go/bigquery/storage/managedwriter"
	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/pdata/pmetric"
	"go.uber.org/zap"
	"google.golang.org/api/googleapi"
	"google.golang.org/api/option"
	"google.golang.org/protobuf/reflect/protoreflect"

	"github.com/open-telemetry/opentelemetry-collector-contrib/exporter/bigquerymetricsexporter/internal/metrics"
)

// metricTypeConfig maps pmetric.MetricType to BQ schema suffix and model constructor.
var metricTypeConfigs = []struct {
	metricType  pmetric.MetricType
	suffix      string
	schema      bigquery.Schema
	newModel    func(protoreflect.MessageDescriptor) metrics.MetricsModel
}{
	{pmetric.MetricTypeGauge, "_gauge", metrics.SchemaGauge, metrics.NewGaugeMetrics},
	{pmetric.MetricTypeSum, "_sum", metrics.SchemaSum, metrics.NewSumMetrics},
	{pmetric.MetricTypeHistogram, "_histogram", metrics.SchemaHistogram, metrics.NewHistogramMetrics},
	{pmetric.MetricTypeExponentialHistogram, "_exponential_histogram", metrics.SchemaExponentialHistogram, metrics.NewExponentialHistogramMetrics},
	{pmetric.MetricTypeSummary, "_summary", metrics.SchemaSummary, metrics.NewSummaryMetrics},
}

type metricsExporter struct {
	cfg          *Config
	logger       *zap.Logger
	client       *bigquery.Client
	writerClient *managedwriter.Client
	streams      map[pmetric.MetricType]*managedwriter.ManagedStream
	descriptors  map[pmetric.MetricType]protoreflect.MessageDescriptor
	modelFactory map[pmetric.MetricType]func(protoreflect.MessageDescriptor) metrics.MetricsModel
}

func newMetricsExporter(cfg *Config, logger *zap.Logger) *metricsExporter {
	return &metricsExporter{
		cfg:    cfg,
		logger: logger,
	}
}

func (e *metricsExporter) start(ctx context.Context, _ component.Host) error {
	opts := e.clientOptions()

	startCtx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()

	// Create BigQuery admin client for schema management.
	client, err := bigquery.NewClient(startCtx, e.cfg.ProjectID, opts...)
	if err != nil {
		return fmt.Errorf("failed to create BigQuery client: %w", err)
	}
	e.client = client

	if e.cfg.CreateSchema {
		if err := e.ensureSchema(ctx); err != nil {
			_ = client.Close()
			e.client = nil
			return fmt.Errorf("failed to create schema: %w", err)
		}
	}

	// Create Storage Write API client.
	writerClient, err := managedwriter.NewClient(ctx, e.cfg.ProjectID, opts...)
	if err != nil {
		_ = client.Close()
		e.client = nil
		return fmt.Errorf("failed to create managedwriter client: %w", err)
	}
	e.writerClient = writerClient

	// Set up streams, descriptors, and model factories per metric type.
	e.streams = make(map[pmetric.MetricType]*managedwriter.ManagedStream, len(metricTypeConfigs))
	e.descriptors = make(map[pmetric.MetricType]protoreflect.MessageDescriptor, len(metricTypeConfigs))
	e.modelFactory = make(map[pmetric.MetricType]func(protoreflect.MessageDescriptor) metrics.MetricsModel, len(metricTypeConfigs))

	for _, tc := range metricTypeConfigs {
		msgDesc, dp, err := metrics.SchemaToDescriptor(tc.schema)
		if err != nil {
			e.closeWriter()
			return fmt.Errorf("failed to create descriptor for %s: %w", tc.suffix, err)
		}

		tablePath := managedwriter.TableParentFromParts(
			e.cfg.ProjectID,
			e.cfg.Dataset,
			e.cfg.TableName(tc.suffix),
		)

		stream, err := e.writerClient.NewManagedStream(ctx,
			managedwriter.WithDestinationTable(tablePath),
			managedwriter.WithType(managedwriter.DefaultStream),
			managedwriter.WithSchemaDescriptor(dp),
			managedwriter.EnableWriteRetries(true),
		)
		if err != nil {
			e.closeWriter()
			return fmt.Errorf("failed to create stream for %s: %w", tc.suffix, err)
		}

		e.streams[tc.metricType] = stream
		e.descriptors[tc.metricType] = msgDesc
		e.modelFactory[tc.metricType] = tc.newModel
	}

	e.logger.Info("BigQuery metrics exporter started",
		zap.String("project", e.cfg.ProjectID),
		zap.String("dataset", e.cfg.Dataset),
	)
	return nil
}

func (e *metricsExporter) shutdown(_ context.Context) error {
	e.closeWriter()
	if e.client != nil {
		return e.client.Close()
	}
	return nil
}

func (e *metricsExporter) closeWriter() {
	for _, stream := range e.streams {
		_ = stream.Close()
	}
	e.streams = nil
	if e.writerClient != nil {
		_ = e.writerClient.Close()
		e.writerClient = nil
	}
}

func (e *metricsExporter) pushMetricsData(ctx context.Context, md pmetric.Metrics) error {
	// Create fresh model instances for this batch.
	models := make(map[pmetric.MetricType]metrics.MetricsModel, len(e.descriptors))
	for mt, desc := range e.descriptors {
		models[mt] = e.modelFactory[mt](desc)
	}

	return metrics.InsertMetrics(ctx, e.logger, md, models, e.streams)
}

func (e *metricsExporter) clientOptions() []option.ClientOption {
	var opts []option.ClientOption
	if e.cfg.CredentialsFile != "" {
		opts = append(opts, option.WithAuthCredentialsFile(option.ServiceAccount, e.cfg.CredentialsFile))
	}
	return opts
}

func (e *metricsExporter) ensureSchema(ctx context.Context) error {
	dataset := e.client.Dataset(e.cfg.Dataset)

	if err := e.ensureDataset(ctx, dataset); err != nil {
		return err
	}

	for suffix, schema := range metrics.MetricTypeSchemas {
		tableName := e.cfg.TableName(suffix)
		if err := e.ensureTable(ctx, dataset, tableName, schema); err != nil {
			return err
		}
	}

	return nil
}

func (e *metricsExporter) ensureDataset(ctx context.Context, dataset *bigquery.Dataset) error {
	md := &bigquery.DatasetMetadata{}
	if e.cfg.Location != "" {
		md.Location = e.cfg.Location
	}

	if err := dataset.Create(ctx, md); err != nil {
		if isAlreadyExists(err) {
			e.logger.Debug("Dataset already exists", zap.String("dataset", e.cfg.Dataset))
			return nil
		}
		return fmt.Errorf("failed to create dataset %q: %w", e.cfg.Dataset, err)
	}

	e.logger.Info("Created dataset", zap.String("dataset", e.cfg.Dataset))
	return nil
}

func (e *metricsExporter) ensureTable(ctx context.Context, dataset *bigquery.Dataset, tableName string, schema bigquery.Schema) error {
	table := dataset.Table(tableName)

	md := &bigquery.TableMetadata{
		Schema: schema,
		TimePartitioning: &bigquery.TimePartitioning{
			Type:  partitionType(e.cfg.PartitionGranularity),
			Field: "time_unix",
		},
		Clustering: &bigquery.Clustering{
			Fields: []string{"service_name", "metric_name"},
		},
	}

	if e.cfg.PartitionExpirationDays > 0 {
		md.TimePartitioning.Expiration = time.Duration(e.cfg.PartitionExpirationDays) * 24 * time.Hour
	}

	if err := table.Create(ctx, md); err != nil {
		if isAlreadyExists(err) {
			e.logger.Debug("Table already exists", zap.String("table", tableName))
			return nil
		}
		return fmt.Errorf("failed to create table %q: %w", tableName, err)
	}

	e.logger.Info("Created table",
		zap.String("table", tableName),
		zap.String("partition", e.cfg.PartitionGranularity),
	)
	return nil
}

func partitionType(granularity string) bigquery.TimePartitioningType {
	switch granularity {
	case "HOUR":
		return bigquery.HourPartitioningType
	case "MONTH":
		return bigquery.MonthPartitioningType
	default:
		return bigquery.DayPartitioningType
	}
}

func isAlreadyExists(err error) bool {
	if e, ok := err.(*googleapi.Error); ok {
		return e.Code == 409
	}
	return false
}

