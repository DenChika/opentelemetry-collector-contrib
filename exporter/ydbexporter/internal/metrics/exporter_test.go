package metrics

import (
	"context"
	"github.com/open-telemetry/opentelemetry-collector-contrib/exporter/ydbexporter/internal/config"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/pmetric"
	conventions "go.opentelemetry.io/collector/semconv/v1.18.0"
	"go.uber.org/zap/zaptest"
	"testing"
	"time"
)

const defaultEndpoint = "grpc://localhost:2136"

func TestMetricsExporter_New(t *testing.T) {
	type validate func(*testing.T, *Exporter, error)

	_ = func() validate {
		return func(t *testing.T, exporter *Exporter, err error) {
			require.NotNil(t, err)
		}
	}

	success := func() validate {
		return func(t *testing.T, exporter *Exporter, err error) {
			require.NoError(t, err)
		}
	}

	tests := map[string]struct {
		config *config.Config
		want   validate
	}{
		"default config": {
			config: config.WithDefaultConfig(),
			want:   success(),
		},
	}

	for name, test := range tests {
		t.Run(name, func(t *testing.T) {
			exporter, err := NewExporter(zaptest.NewLogger(t), test.config)
			test.want(t, exporter, err)
		})
	}
}

func TestMetricsExporter_createTable(t *testing.T) {
	createTable := func(t *testing.T, endpoint string) error {
		exporter, err := NewExporter(zaptest.NewLogger(t), withTestExporterConfig()(endpoint))
		require.NoError(t, err)
		return exporter.createTable(context.TODO())
	}

	t.Run("create table success", func(t *testing.T) {
		err := createTable(t, defaultEndpoint)
		require.NoError(t, err)
	})
}

func TestTracesExporter_PushData(t *testing.T) {
	t.Run("test check gauge metric metadata", func(t *testing.T) {
		assertMetricData(t, func(t *testing.T, exporter *loggingExporter) {
			mustPushMetricsData(t, exporter, simpleGaugeMetrics(1))
			gauge := exporter.lastUpsertedMetric.Gauge().DataPoints().At(0)

			require.Equal(t, "gauge metrics", exporter.lastUpsertedMetric.Name())
			require.Equal(t, "gauge", exporter.lastUpsertedMetric.Unit())
			require.Equal(t, "this is a gauge metrics", exporter.lastUpsertedMetric.Description())

			require.Equal(t, int64(1), gauge.IntValue())
			require.Equal(t, pmetric.DataPointFlags(2), gauge.Flags())

			v, ok := gauge.Attributes().Get("gauge")
			require.True(t, ok)
			require.Equal(t, "gauge1", v.Str())

			exemplars := gauge.Exemplars().At(0)
			require.Equal(t, int64(3), exemplars.IntValue())
			require.Equal(t, pcommon.SpanID{1, 2}, exemplars.SpanID())
			require.Equal(t, pcommon.TraceID{3, 4}, exemplars.TraceID())

			v, ok = exemplars.FilteredAttributes().Get("key_g")
			require.True(t, ok)
			require.Equal(t, "value_g", v.Str())
		})
	})
	t.Run("test check sum metric metadata", func(t *testing.T) {
		assertMetricData(t, func(t *testing.T, exporter *loggingExporter) {
			mustPushMetricsData(t, exporter, simpleSumMetrics(1))
			sum := exporter.lastUpsertedMetric.Sum().DataPoints().At(0)

			require.Equal(t, "sum metrics", exporter.lastUpsertedMetric.Name())
			require.Equal(t, "sum", exporter.lastUpsertedMetric.Unit())
			require.Equal(t, "this is a sum metrics", exporter.lastUpsertedMetric.Description())

			require.Equal(t, int64(2), sum.IntValue())
			require.Equal(t, pmetric.DataPointFlags(3), sum.Flags())

			v, ok := sum.Attributes().Get("sum")
			require.True(t, ok)
			require.Equal(t, "sum1", v.Str())

			exemplars := sum.Exemplars().At(0)
			require.Equal(t, int64(4), exemplars.IntValue())
			require.Equal(t, pcommon.SpanID{2, 3}, exemplars.SpanID())
			require.Equal(t, pcommon.TraceID{4, 5}, exemplars.TraceID())

			v, ok = exemplars.FilteredAttributes().Get("key_sum")
			require.True(t, ok)
			require.Equal(t, "value_sum", v.Str())
		})
	})
	t.Run("test check histogram metric metadata", func(t *testing.T) {
		assertMetricData(t, func(t *testing.T, exporter *loggingExporter) {
			mustPushMetricsData(t, exporter, simpleHistogramMetrics(1))
			histogram := exporter.lastUpsertedMetric.Histogram().DataPoints().At(0)

			require.Equal(t, "histogram metrics", exporter.lastUpsertedMetric.Name())
			require.Equal(t, "histogram", exporter.lastUpsertedMetric.Unit())
			require.Equal(t, "this is a histogram metrics", exporter.lastUpsertedMetric.Description())

			require.Equal(t, uint64(3), histogram.Count())
			require.Equal(t, float64(4), histogram.Sum())
			require.Equal(t, pmetric.DataPointFlags(5), histogram.Flags())
			require.Equal(t, float64(0), histogram.Min())
			require.Equal(t, float64(3), histogram.Max())
			require.Equal(t, "[0,0,0,0,0]", getListValues(histogram.ExplicitBounds().AsRaw()))
			require.Equal(t, "[0,0,0,1,0]", getListValues(histogram.BucketCounts().AsRaw()))

			v, ok := histogram.Attributes().Get("histogram")
			require.True(t, ok)
			require.Equal(t, "histogram1", v.Str())

			exemplars := histogram.Exemplars().At(0)
			require.Equal(t, int64(5), exemplars.IntValue())
			require.Equal(t, pcommon.SpanID{3, 4}, exemplars.SpanID())
			require.Equal(t, pcommon.TraceID{5, 6}, exemplars.TraceID())

			v, ok = exemplars.FilteredAttributes().Get("key_h")
			require.True(t, ok)
			require.Equal(t, "value_h", v.Str())
		})
	})
	t.Run("test check exponential histogram metric metadata", func(t *testing.T) {
		assertMetricData(t, func(t *testing.T, exporter *loggingExporter) {
			mustPushMetricsData(t, exporter, simpleExpHistogramMetrics(1))
			expHistogram := exporter.lastUpsertedMetric.ExponentialHistogram().DataPoints().At(0)

			require.Equal(t, "exponential histogram metrics", exporter.lastUpsertedMetric.Name())
			require.Equal(t, "exponential histogram", exporter.lastUpsertedMetric.Unit())
			require.Equal(t, "this is an exponential histogram metrics", exporter.lastUpsertedMetric.Description())

			require.Equal(t, float64(4), expHistogram.Sum())
			require.Equal(t, float64(1), expHistogram.Min())
			require.Equal(t, float64(5), expHistogram.Max())
			require.Equal(t, uint64(6), expHistogram.ZeroCount())
			require.Equal(t, uint64(7), expHistogram.Count())
			require.Equal(t, pmetric.DataPointFlags(8), expHistogram.Flags())
			require.Equal(t, int32(9), expHistogram.Scale())
			require.Equal(t, int32(1), expHistogram.Negative().Offset())
			require.Equal(t, int32(2), expHistogram.Positive().Offset())
			require.Equal(t, "[0,0,0,1,0]", getListValues(expHistogram.Negative().BucketCounts().AsRaw()))
			require.Equal(t, "[0,1,0,0,0]", getListValues(expHistogram.Positive().BucketCounts().AsRaw()))

			v, ok := expHistogram.Attributes().Get("exp_histogram")
			require.True(t, ok)
			require.Equal(t, "exp_histogram1", v.Str())

			exemplars := expHistogram.Exemplars().At(0)
			require.Equal(t, int64(9), exemplars.IntValue())
			require.Equal(t, pcommon.SpanID{4, 5}, exemplars.SpanID())
			require.Equal(t, pcommon.TraceID{6, 7}, exemplars.TraceID())

			v, ok = exemplars.FilteredAttributes().Get("key_eh")
			require.True(t, ok)
			require.Equal(t, "value_eh", v.Str())
		})
	})
	t.Run("test check summary metric metadata", func(t *testing.T) {
		assertMetricData(t, func(t *testing.T, exporter *loggingExporter) {
			mustPushMetricsData(t, exporter, simpleSummaryMetrics(1))
			summary := exporter.lastUpsertedMetric.Summary().DataPoints().At(0)

			require.Equal(t, "summary metrics", exporter.lastUpsertedMetric.Name())
			require.Equal(t, "summary", exporter.lastUpsertedMetric.Unit())
			require.Equal(t, "this is a summary metrics", exporter.lastUpsertedMetric.Description())

			require.Equal(t, uint64(4), summary.Count())
			require.Equal(t, float64(5), summary.Sum())
			require.Equal(t, pmetric.DataPointFlags(6), summary.Flags())

			v, ok := summary.Attributes().Get("summary")
			require.True(t, ok)
			require.Equal(t, "summary1", v.Str())

			quantile := summary.QuantileValues().At(0)
			require.Equal(t, float64(7), quantile.Value())
			require.Equal(t, float64(8), quantile.Quantile())
		})
	})
	t.Run("test check resource metadata", func(t *testing.T) {
		assertMetricData(t, func(t *testing.T, exporter *loggingExporter) {
			mustPushMetricsData(t, exporter, simpleGaugeMetrics(1))

			require.Equal(t, "Resource SchemaUrl", exporter.lastUpsertedResourceMetric.SchemaUrl())
		})
	})
	t.Run("test check scope metadata", func(t *testing.T) {
		assertMetricData(t, func(t *testing.T, exporter *loggingExporter) {
			mustPushMetricsData(t, exporter, simpleGaugeMetrics(1))

			require.Equal(t, "Scope SchemaUrl", exporter.lastUpsertedScopeMetric.SchemaUrl())
			require.Equal(t, "Scope name", exporter.lastUpsertedScopeMetric.Scope().Name())
			require.Equal(t, "Scope version", exporter.lastUpsertedScopeMetric.Scope().Version())
		})
	})
	t.Run("test check resource attributes", func(t *testing.T) {
		assertMetricData(t, func(t *testing.T, exporter *loggingExporter) {
			mustPushMetricsData(t, exporter, simpleGaugeMetrics(1))

			v, ok := exporter.lastUpsertedResourceMetric.Resource().Attributes().Get(conventions.AttributeServiceName)
			require.True(t, ok)
			require.Equal(t, "test-service", v.Str())
		})
	})
	t.Run("test check scope attributes", func(t *testing.T) {
		assertMetricData(t, func(t *testing.T, exporter *loggingExporter) {
			mustPushMetricsData(t, exporter, simpleGaugeMetrics(1))

			v, ok := exporter.lastUpsertedScopeMetric.Scope().Attributes().Get("Scope Attribute")
			require.True(t, ok)
			require.Equal(t, "value", v.Str())
		})
	})
	t.Run("push data more than once success", func(t *testing.T) {
		exporter := newTestMetricsExporter(t, defaultEndpoint)
		loggingExporter := newLoggingExporter(exporter)

		mustPushMetricsData(t, loggingExporter, simpleGaugeMetrics(2))
		mustPushMetricsData(t, loggingExporter, simpleSumMetrics(2))
		mustPushMetricsData(t, loggingExporter, simpleHistogramMetrics(2))
		mustPushMetricsData(t, loggingExporter, simpleExpHistogramMetrics(2))
		mustPushMetricsData(t, loggingExporter, simpleSummaryMetrics(2))
		require.Equal(t, 10, loggingExporter.rows)
	})
}

func assertMetricData(t *testing.T, assert func(*testing.T, *loggingExporter)) {
	exporter := newTestMetricsExporter(t, defaultEndpoint)
	loggingExporter := newLoggingExporter(exporter)

	assert(t, loggingExporter)
}

func newTestMetricsExporter(t *testing.T, dsn string, fns ...func(*config.Config)) *Exporter {
	exporter, err := NewExporter(zaptest.NewLogger(t), withTestExporterConfig(fns...)(dsn))
	require.NoError(t, err)
	require.NoError(t, exporter.Start(context.TODO(), nil))

	t.Cleanup(func() {
		err = exporter.Shutdown(context.TODO())
		require.NoError(t, err)
	})
	return exporter
}

func withTestExporterConfig(fns ...func(*config.Config)) func(string) *config.Config {
	return func(endpoint string) *config.Config {
		var configMods []func(*config.Config)
		configMods = append(configMods, func(cfg *config.Config) {
			cfg.Endpoint = endpoint
		})
		configMods = append(configMods, fns...)
		return config.WithDefaultConfig(configMods...)
	}
}

func simpleResourcesAndScopes(metrics pmetric.Metrics) pmetric.ScopeMetrics {
	rm := metrics.ResourceMetrics().AppendEmpty()
	rm.Resource().Attributes().PutStr(conventions.AttributeServiceName, "test-service")
	rm.SetSchemaUrl("Resource SchemaUrl")

	sm := rm.ScopeMetrics().AppendEmpty()
	sm.SetSchemaUrl("Scope SchemaUrl")
	sm.Scope().Attributes().PutStr("Scope Attribute", "value")
	sm.Scope().SetName("Scope name")
	sm.Scope().SetVersion("Scope version")

	return sm
}

func simpleGaugeMetrics(count int) pmetric.Metrics {
	metrics := pmetric.NewMetrics()
	sm := simpleResourcesAndScopes(metrics)

	for i := 0; i < count; i++ {
		m := sm.Metrics().AppendEmpty()
		m.SetName("gauge metrics")
		m.SetUnit("gauge")
		m.SetDescription("this is a gauge metrics")

		dp := m.SetEmptyGauge().DataPoints().AppendEmpty()
		dp.SetIntValue(1)
		dp.SetFlags(2)
		dp.Attributes().PutStr("gauge", "gauge1")
		dp.SetTimestamp(pcommon.NewTimestampFromTime(time.Now()))

		exemplars := dp.Exemplars().AppendEmpty()
		exemplars.SetIntValue(3)
		exemplars.FilteredAttributes().PutStr("key_g", "value_g")
		exemplars.SetSpanID([8]byte{1, 2})
		exemplars.SetTraceID([16]byte{3, 4})
	}

	return metrics
}

func simpleSumMetrics(count int) pmetric.Metrics {
	metrics := pmetric.NewMetrics()
	sm := simpleResourcesAndScopes(metrics)

	for i := 0; i < count; i++ {
		m := sm.Metrics().AppendEmpty()
		m.SetName("sum metrics")
		m.SetUnit("sum")
		m.SetDescription("this is a sum metrics")

		dp := m.SetEmptySum().DataPoints().AppendEmpty()
		dp.SetIntValue(2)
		dp.SetFlags(3)
		dp.Attributes().PutStr("sum", "sum1")
		dp.SetTimestamp(pcommon.NewTimestampFromTime(time.Now()))

		exemplars := dp.Exemplars().AppendEmpty()
		exemplars.SetIntValue(4)
		exemplars.FilteredAttributes().PutStr("key_sum", "value_sum")
		exemplars.SetSpanID([8]byte{2, 3})
		exemplars.SetTraceID([16]byte{4, 5})
	}

	return metrics
}

func simpleHistogramMetrics(count int) pmetric.Metrics {
	metrics := pmetric.NewMetrics()
	sm := simpleResourcesAndScopes(metrics)

	for i := 0; i < count; i++ {
		m := sm.Metrics().AppendEmpty()
		m.SetName("histogram metrics")
		m.SetUnit("histogram")
		m.SetDescription("this is a histogram metrics")

		dp := m.SetEmptyHistogram().DataPoints().AppendEmpty()
		dp.SetStartTimestamp(pcommon.NewTimestampFromTime(time.Now()))
		dp.SetTimestamp(pcommon.NewTimestampFromTime(time.Now()))
		dp.SetCount(3)
		dp.SetSum(4)
		dp.SetFlags(5)
		dp.Attributes().PutStr("histogram", "histogram1")
		dp.ExplicitBounds().FromRaw([]float64{0, 0, 0, 0, 0})
		dp.BucketCounts().FromRaw([]uint64{0, 0, 0, 1, 0})
		dp.SetMin(0)
		dp.SetMax(3)

		exemplars := dp.Exemplars().AppendEmpty()
		exemplars.SetIntValue(5)
		exemplars.FilteredAttributes().PutStr("key_h", "value_h")
		exemplars.SetSpanID([8]byte{3, 4})
		exemplars.SetTraceID([16]byte{5, 6})
	}

	return metrics
}

func simpleExpHistogramMetrics(count int) pmetric.Metrics {
	metrics := pmetric.NewMetrics()
	sm := simpleResourcesAndScopes(metrics)

	for i := 0; i < count; i++ {
		m := sm.Metrics().AppendEmpty()
		m.SetName("exponential histogram metrics")
		m.SetUnit("exponential histogram")
		m.SetDescription("this is an exponential histogram metrics")

		dp := m.SetEmptyExponentialHistogram().DataPoints().AppendEmpty()
		dp.SetStartTimestamp(pcommon.NewTimestampFromTime(time.Now()))
		dp.SetTimestamp(pcommon.NewTimestampFromTime(time.Now()))
		dp.SetSum(4)
		dp.SetMin(1)
		dp.SetMax(5)
		dp.SetZeroCount(6)
		dp.SetCount(7)
		dp.SetFlags(8)
		dp.SetScale(9)
		dp.Attributes().PutStr("exp_histogram", "exp_histogram1")
		dp.Negative().SetOffset(1)
		dp.Negative().BucketCounts().FromRaw([]uint64{0, 0, 0, 1, 0})
		dp.Positive().SetOffset(2)
		dp.Positive().BucketCounts().FromRaw([]uint64{0, 1, 0, 0, 0})

		exemplars := dp.Exemplars().AppendEmpty()
		exemplars.SetIntValue(9)
		exemplars.FilteredAttributes().PutStr("key_eh", "value_eh")
		exemplars.SetSpanID([8]byte{4, 5})
		exemplars.SetTraceID([16]byte{6, 7})
	}

	return metrics
}

func simpleSummaryMetrics(count int) pmetric.Metrics {
	metrics := pmetric.NewMetrics()
	sm := simpleResourcesAndScopes(metrics)

	for i := 0; i < count; i++ {
		m := sm.Metrics().AppendEmpty()
		m.SetName("summary metrics")
		m.SetUnit("summary")
		m.SetDescription("this is a summary metrics")

		dp := m.SetEmptySummary().DataPoints().AppendEmpty()
		dp.SetStartTimestamp(pcommon.NewTimestampFromTime(time.Now()))
		dp.SetTimestamp(pcommon.NewTimestampFromTime(time.Now()))
		dp.Attributes().PutStr("summary", "summary1")
		dp.SetCount(4)
		dp.SetSum(5)
		dp.SetFlags(6)

		quantileValues := dp.QuantileValues().AppendEmpty()
		quantileValues.SetValue(7)
		quantileValues.SetQuantile(8)
	}

	return metrics
}

func mustPushMetricsData(t *testing.T, exporter *loggingExporter, md pmetric.Metrics) {
	err := exporter.pushData(context.TODO(), md)
	require.NoError(t, err)
}

type loggingExporter struct {
	exporter                   *Exporter
	rows                       int
	lastUpsertedResourceMetric pmetric.ResourceMetrics
	lastUpsertedScopeMetric    pmetric.ScopeMetrics
	lastUpsertedMetric         pmetric.Metric
}

func newLoggingExporter(exporter *Exporter) *loggingExporter {
	return &loggingExporter{exporter: exporter}
}

func (l *loggingExporter) pushData(ctx context.Context, md pmetric.Metrics) error {
	for i := 0; i < md.ResourceMetrics().Len(); i++ {
		resourceMetrics := md.ResourceMetrics().At(i)
		for j := 0; j < resourceMetrics.ScopeMetrics().Len(); j++ {
			scopeMetrics := resourceMetrics.ScopeMetrics().At(j)
			for k := 0; k < scopeMetrics.Metrics().Len(); k++ {
				metric := scopeMetrics.Metrics().At(k)

				l.lastUpsertedResourceMetric = resourceMetrics
				l.lastUpsertedScopeMetric = scopeMetrics
				l.lastUpsertedMetric = metric
				l.rows++
			}
		}
	}

	return l.exporter.PushData(ctx, md)
}
