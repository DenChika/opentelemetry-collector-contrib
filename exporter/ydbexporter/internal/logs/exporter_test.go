// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package logs

import (
	"context"
	"github.com/open-telemetry/opentelemetry-collector-contrib/exporter/ydbexporter/internal/config"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/plog"
	conventions "go.opentelemetry.io/collector/semconv/v1.18.0"
	"go.uber.org/zap/zaptest"
)

const defaultEndpoint = "grpc://localhost:2136"

func TestLogsExporter_New(t *testing.T) {
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

func TestLogsExporter_createTable(t *testing.T) {
	createTable := func(t *testing.T, endpoint string) error {
		exporter := newTestLogsExporter(t, defaultEndpoint)
		return exporter.createTable(context.TODO())
	}

	t.Run("create table success", func(t *testing.T) {
		err := createTable(t, defaultEndpoint)
		require.NoError(t, err)
	})
}

func TestLogsExporter_PushData(t *testing.T) {
	t.Run("test check records metadata", func(t *testing.T) {
		assertRecordData(t, func(t *testing.T, exporter *loggingExporter) {
			mustPushLogsData(t, exporter, simpleLogs(1))

			require.Equal(t, plog.SeverityNumber(1), exporter.lastUpsertedRecord.SeverityNumber())
			require.Equal(t, "text", exporter.lastUpsertedRecord.SeverityText())
			require.Equal(t, pcommon.SpanID{2, 3}, exporter.lastUpsertedRecord.SpanID())
			require.Equal(t, pcommon.TraceID{4, 5}, exporter.lastUpsertedRecord.TraceID())
			require.Equal(t, plog.LogRecordFlags(6), exporter.lastUpsertedRecord.Flags())
		})
	})
	t.Run("test check resource metadata", func(t *testing.T) {
		assertRecordData(t, func(t *testing.T, exporter *loggingExporter) {
			mustPushLogsData(t, exporter, simpleLogs(1))

			require.Equal(t,
				"https://opentelemetry.io/schemas/1.4.0",
				exporter.lastUpsertedResourceLog.SchemaUrl())
		})
	})
	t.Run("test check scope metadata", func(t *testing.T) {
		assertRecordData(t, func(t *testing.T, exporter *loggingExporter) {
			mustPushLogsData(t, exporter, simpleLogs(1))

			require.Equal(t,
				"https://opentelemetry.io/schemas/1.7.0",
				exporter.lastUpsertedScopeLog.SchemaUrl())

			require.Equal(t,
				"io.opentelemetry.contrib.ydb",
				exporter.lastUpsertedScopeLog.Scope().Name())

			require.Equal(t, "1.0.0", exporter.lastUpsertedScopeLog.Scope().Version())
		})
	})
	t.Run("test check record attributes", func(t *testing.T) {
		assertRecordData(t, func(t *testing.T, exporter *loggingExporter) {
			mustPushLogsData(t, exporter, simpleLogs(1))

			v, ok := exporter.lastUpsertedRecord.Attributes().Get(conventions.AttributeServiceName)
			require.True(t, ok)
			require.Equal(t, "v", v.Str())
		})
	})
	t.Run("test check resource attributes", func(t *testing.T) {
		assertRecordData(t, func(t *testing.T, exporter *loggingExporter) {
			mustPushLogsData(t, exporter, simpleLogs(1))

			v, ok := exporter.lastUpsertedResourceLog.Resource().Attributes().Get(conventions.AttributeServiceName)
			require.True(t, ok)
			require.Equal(t, "test-service", v.Str())
		})
	})
	t.Run("test check scope attributes", func(t *testing.T) {
		assertRecordData(t, func(t *testing.T, exporter *loggingExporter) {
			mustPushLogsData(t, exporter, simpleLogs(1))

			v, ok := exporter.lastUpsertedScopeLog.Scope().Attributes().Get("lib")
			require.True(t, ok)
			require.Equal(t, "ydb", v.Str())
		})
	})
	t.Run("push data more than once success", func(t *testing.T) {
		exporter := newTestLogsExporter(t, defaultEndpoint)
		loggingExporter := newLoggingExporter(exporter)

		mustPushLogsData(t, loggingExporter, simpleLogs(1))
		mustPushLogsData(t, loggingExporter, simpleLogs(2))
		require.Equal(t, 3, loggingExporter.rows)
	})
}

func assertRecordData(t *testing.T, assert func(*testing.T, *loggingExporter)) {
	exporter := newTestLogsExporter(t, defaultEndpoint)
	loggingExporter := newLoggingExporter(exporter)

	assert(t, loggingExporter)
}

func newTestLogsExporter(t *testing.T, dsn string, fns ...func(*config.Config)) *Exporter {
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

func simpleLogs(count int) plog.Logs {
	logs := plog.NewLogs()
	rl := logs.ResourceLogs().AppendEmpty()
	rl.SetSchemaUrl("https://opentelemetry.io/schemas/1.4.0")
	rl.Resource().Attributes().PutStr(conventions.AttributeServiceName, "test-service")
	sl := rl.ScopeLogs().AppendEmpty()
	sl.SetSchemaUrl("https://opentelemetry.io/schemas/1.7.0")
	sl.Scope().SetName("io.opentelemetry.contrib.ydb")
	sl.Scope().SetVersion("1.0.0")
	sl.Scope().Attributes().PutStr("lib", "ydb")
	for i := 0; i < count; i++ {
		r := sl.LogRecords().AppendEmpty()
		r.SetTimestamp(pcommon.NewTimestampFromTime(time.Now()))
		r.SetSeverityNumber(1)
		r.SetSeverityText("text")
		r.SetSpanID([8]byte{2, 3})
		r.SetTraceID([16]byte{4, 5})
		r.SetFlags(6)
		r.Attributes().PutStr(conventions.AttributeServiceName, "v")
	}
	return logs
}

func mustPushLogsData(t *testing.T, exporter *loggingExporter, ld plog.Logs) {
	err := exporter.pushData(context.TODO(), ld)
	require.NoError(t, err)
}

type loggingExporter struct {
	exporter                *Exporter
	rows                    int
	lastUpsertedResourceLog plog.ResourceLogs
	lastUpsertedRecord      plog.LogRecord
	lastUpsertedScopeLog    plog.ScopeLogs
}

func newLoggingExporter(exporter *Exporter) *loggingExporter {
	return &loggingExporter{exporter: exporter}
}

func (l *loggingExporter) pushData(ctx context.Context, ld plog.Logs) error {
	for i := 0; i < ld.ResourceLogs().Len(); i++ {
		resourceLog := ld.ResourceLogs().At(i)
		for j := 0; j < resourceLog.ScopeLogs().Len(); j++ {
			scopeLog := resourceLog.ScopeLogs().At(j)
			for k := 0; k < scopeLog.LogRecords().Len(); k++ {
				record := scopeLog.LogRecords().At(k)

				l.lastUpsertedResourceLog = resourceLog
				l.lastUpsertedRecord = record
				l.lastUpsertedScopeLog = scopeLog
				l.rows++
			}
		}
	}

	return l.exporter.PushData(ctx, ld)
}
