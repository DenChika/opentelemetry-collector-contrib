package metrics

import (
	"encoding/json"
	"github.com/google/uuid"
	"github.com/open-telemetry/opentelemetry-collector-contrib/exporter/ydbexporter/internal/config"
	"github.com/ydb-platform/ydb-go-sdk/v3/table/options"
	"github.com/ydb-platform/ydb-go-sdk/v3/table/types"
	"go.opentelemetry.io/collector/pdata/pmetric"
	"reflect"
)

type histogram struct {
}

func (g *histogram) tableName(config *config.TableConfig) string {
	return config.Name + "_histogram"
}

func (g *histogram) createTableOptions(config *config.TableConfig) []options.CreateTableOption {
	opts := []options.CreateTableOption{
		options.WithColumn("timestamp", types.TypeTimestamp),
		options.WithColumn("metricName", types.TypeUTF8),
		options.WithColumn("uuid", types.TypeUTF8),
		options.WithColumn("startTimestamp", types.Optional(types.TypeTimestamp)),

		options.WithColumn("resourceAttributes", types.Optional(types.TypeJSONDocument)),
		options.WithColumn("resourceSchemaUrl", types.Optional(types.TypeUTF8)),
		options.WithColumn("scopeName", types.Optional(types.TypeUTF8)),
		options.WithColumn("scopeVersion", types.Optional(types.TypeUTF8)),
		options.WithColumn("scopeAttributes", types.Optional(types.TypeJSONDocument)),
		options.WithColumn("scopeSchemaUrl", types.Optional(types.TypeUTF8)),

		options.WithColumn("metricDescription", types.Optional(types.TypeUTF8)),
		options.WithColumn("metricUnit", types.Optional(types.TypeUTF8)),
		options.WithColumn("attributes", types.Optional(types.TypeJSONDocument)),
		options.WithColumn("startTimeUnix", types.Optional(types.TypeTimestamp)),
		options.WithColumn("timeUnix", types.Optional(types.TypeTimestamp)),

		options.WithColumn("count", types.Optional(types.TypeUint64)),
		options.WithColumn("sum", types.Optional(types.TypeDouble)),
		options.WithColumn("bucketsCount", types.Optional(types.List(types.TypeUint64))),
		options.WithColumn("explicitBounds", types.Optional(types.List(types.TypeDouble))),

		options.WithColumn("flags", types.Optional(types.TypeUint32)),
		options.WithColumn("min", types.Optional(types.TypeDouble)),
		options.WithColumn("max", types.Optional(types.TypeDouble)),
		options.WithColumn("exemplars", types.Optional(types.TypeJSONDocument)),

		options.WithPrimaryKeyColumn("timestamp", "uuid", "metricName"),
		options.WithAttribute("STORE", "COLUMN"),
	}

	partitionOptions := []options.PartitioningSettingsOption{
		options.WithPartitioningBy([]string{"HASH(timestamp)"}),
	}
	if config.PartitionsCount > 0 {
		partitionOptions = append(partitionOptions, options.WithMinPartitionsCount(config.PartitionsCount))
	}
	opts = append(opts, options.WithPartitioningSettings(partitionOptions...))

	if config.TTL > 0 {
		opts = append(opts,
			options.WithTimeToLiveSettings(
				options.NewTTLSettings().
					ColumnDateType("timestamp").
					ExpireAfter(config.TTL)))
	}
	return opts
}

func (g *histogram) createRecords(resourceMetrics pmetric.ResourceMetrics, scopeMetrics pmetric.ScopeMetrics, metric pmetric.Metric) ([]types.Value, error) {
	recordAttributes, err := json.Marshal(resourceMetrics.Resource().Attributes().AsRaw())
	if err != nil {
		return nil, err
	}
	scopeAttributes, err := json.Marshal(scopeMetrics.Scope().Attributes().AsRaw())
	if err != nil {
		return nil, err
	}
	var records []types.Value
	dataPoints := metric.Histogram().DataPoints()
	for i := 0; i < dataPoints.Len(); i++ {
		dp := metric.Histogram().DataPoints().At(i)

		attributes, err := json.Marshal(dp.Attributes().AsRaw())
		if err != nil {
			return nil, err
		}

		exemplars, err := json.Marshal(convertExemplars(dp.Exemplars()))
		if err != nil {
			return nil, err
		}

		record := types.StructValue(
			types.StructFieldValue("startTimestamp", types.TimestampValueFromTime(dp.StartTimestamp().AsTime())),
			types.StructFieldValue("timestamp", types.TimestampValueFromTime(dp.Timestamp().AsTime())),
			types.StructFieldValue("uuid", types.UTF8Value(uuid.New().String())),
			types.StructFieldValue("resourceSchemaUrl", types.UTF8Value(resourceMetrics.SchemaUrl())),
			types.StructFieldValue("resourceAttributes", types.JSONDocumentValueFromBytes(recordAttributes)),
			types.StructFieldValue("scopeName", types.UTF8Value(scopeMetrics.Scope().Name())),
			types.StructFieldValue("scopeVersion", types.UTF8Value(scopeMetrics.Scope().Version())),
			types.StructFieldValue("scopeAttributes", types.JSONDocumentValueFromBytes(scopeAttributes)),
			types.StructFieldValue("scopeSchemaUrl", types.UTF8Value(scopeMetrics.SchemaUrl())),
			types.StructFieldValue("metricName", types.UTF8Value(metric.Name())),
			types.StructFieldValue("metricDescription", types.UTF8Value(metric.Description())),
			types.StructFieldValue("metricUnit", types.UTF8Value(metric.Unit())),
			types.StructFieldValue("attributes", types.JSONDocumentValueFromBytes(attributes)),
			types.StructFieldValue("startTimeUnix", types.DatetimeValueFromTime(dp.StartTimestamp().AsTime())),
			types.StructFieldValue("timeUnix", types.DatetimeValueFromTime(dp.Timestamp().AsTime())),
			types.StructFieldValue("count", types.Uint64Value(dp.Count())),
			types.StructFieldValue("sum", types.DoubleValue(dp.Sum())),
			types.StructFieldValue("bucketsCount", types.ListValue(getListValues(dp.BucketCounts().AsRaw())...)),
			types.StructFieldValue("explicitBounds", types.ListValue(getListValues(dp.ExplicitBounds().AsRaw())...)),
			types.StructFieldValue("flags", types.Uint32Value(uint32(dp.Flags()))),
			types.StructFieldValue("min", types.DoubleValue(dp.Min())),
			types.StructFieldValue("max", types.DoubleValue(dp.Max())),
			types.StructFieldValue("exemplars", types.JSONDocumentValueFromBytes(exemplars)),
		)
		records = append(records, record)
	}
	return records, nil
}

type numeric interface {
	~uint64 | ~float64
}

func getListValues[T numeric](dp []T) []types.Value {
	var value types.Type

	if len(dp) == 0 {
		return []types.Value{}
	}

	switch reflect.TypeOf(dp[0]).Kind() {
	case reflect.Uint64:
		value = types.TypeUint64
	case reflect.Float64:
		value = types.TypeDouble
	default:
		value = types.TypeUnknown
	}

	values := make([]types.Value, len(dp))
	for i, v := range dp {
		switch value {
		case types.TypeUint64:
			values[i] = types.Uint64Value(uint64(v))
		case types.TypeDouble:
			values[i] = types.DoubleValue(float64(v))
		default:
			values[i] = types.ZeroValue(types.TypeUnknown)
		}
	}

	return values
}
