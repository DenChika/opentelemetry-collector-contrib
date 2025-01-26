// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package traces

import (
	"context"
	"github.com/open-telemetry/opentelemetry-collector-contrib/exporter/ydbexporter/internal/config"
	"go.opentelemetry.io/collector/pdata/ptrace"
	"os"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/collector/pdata/pcommon"
	conventions "go.opentelemetry.io/collector/semconv/v1.18.0"
	"go.uber.org/zap/zaptest"
)

const defaultEndpoint = "grpc://localhost:2136"

func TestTracesExporter_New(t *testing.T) {
	if os.Getenv("RUN_DOCKER_TESTS") == "" {
		t.Skip()
	}
	type validate func(*testing.T, *Exporter, error)

	success := func() validate {
		return func(t *testing.T, exporter *Exporter, err error) {
			require.Error(t, nil)
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

func TestTracesExporter_createTable(t *testing.T) {
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
	t.Run("test check span metadata", func(t *testing.T) {
		assertSpanData(t, func(t *testing.T, exporter *loggingExporter) {
			mustPushTracesData(t, exporter, simpleTraces(1))

			startTime := exporter.lastUpsertedSpan.StartTimestamp().AsTime()

			require.Equal(t, time.Minute, exporter.lastUpsertedSpan.EndTimestamp().AsTime().Sub(startTime))
			require.Equal(t, pcommon.SpanID{2, 3}, exporter.lastUpsertedSpan.SpanID())
			require.Equal(t, pcommon.TraceID{4, 5}, exporter.lastUpsertedSpan.TraceID())
			require.Equal(t, pcommon.SpanID{6, 7}, exporter.lastUpsertedSpan.ParentSpanID())
			require.Equal(t, "span1", exporter.lastUpsertedSpan.Name())
			require.Equal(t, ptrace.SpanKind(1), exporter.lastUpsertedSpan.Kind())
		})
	})
	t.Run("test check scope metadata", func(t *testing.T) {
		assertSpanData(t, func(t *testing.T, exporter *loggingExporter) {
			mustPushTracesData(t, exporter, simpleTraces(1))

			require.Equal(t,
				"io.opentelemetry.contrib.ydb",
				exporter.lastUpsertedScopeSpan.Scope().Name())

			require.Equal(t, "1.0.0", exporter.lastUpsertedScopeSpan.Scope().Version())
		})
	})
	t.Run("test check span event metadata", func(t *testing.T) {
		assertSpanData(t, func(t *testing.T, exporter *loggingExporter) {
			mustPushTracesData(t, exporter, simpleTraces(1))

			require.Equal(t, "event1", exporter.lastUpsertedSpan.Events().At(0).Name())
			require.Equal(t, "event2", exporter.lastUpsertedSpan.Events().At(1).Name())
		})
	})
	t.Run("test check span link metadata", func(t *testing.T) {
		assertSpanData(t, func(t *testing.T, exporter *loggingExporter) {
			mustPushTracesData(t, exporter, simpleTraces(1))

			require.Equal(t, pcommon.SpanID{8, 9}, exporter.lastUpsertedSpan.Links().At(0).SpanID())
			require.Equal(t, pcommon.TraceID{10, 11}, exporter.lastUpsertedSpan.Links().At(0).TraceID())
			require.Equal(t, pcommon.SpanID{12, 13}, exporter.lastUpsertedSpan.Links().At(1).SpanID())
			require.Equal(t, pcommon.TraceID{14, 15}, exporter.lastUpsertedSpan.Links().At(1).TraceID())
		})
	})
	t.Run("test check span attributes", func(t *testing.T) {
		assertSpanData(t, func(t *testing.T, exporter *loggingExporter) {
			mustPushTracesData(t, exporter, simpleTraces(1))

			v, ok := exporter.lastUpsertedSpan.Attributes().Get(conventions.AttributeServiceName)
			require.True(t, ok)
			require.Equal(t, "span", v.Str())
		})
	})
	t.Run("test check resource attributes", func(t *testing.T) {
		assertSpanData(t, func(t *testing.T, exporter *loggingExporter) {
			mustPushTracesData(t, exporter, simpleTraces(1))

			v, ok := exporter.lastUpsertedResourceSpan.Resource().Attributes().Get(conventions.AttributeServiceName)
			require.True(t, ok)
			require.Equal(t, "test-service", v.Str())
		})
	})
	t.Run("test check scope attributes", func(t *testing.T) {
		assertSpanData(t, func(t *testing.T, exporter *loggingExporter) {
			mustPushTracesData(t, exporter, simpleTraces(1))

			v, ok := exporter.lastUpsertedScopeSpan.Scope().Attributes().Get("lib")
			require.True(t, ok)
			require.Equal(t, "ydb", v.Str())
		})
	})
	t.Run("test check span event attributes", func(t *testing.T) {
		assertSpanData(t, func(t *testing.T, exporter *loggingExporter) {
			mustPushTracesData(t, exporter, simpleTraces(1))

			v, ok := exporter.lastUpsertedSpan.Events().At(0).Attributes().Get("event1")
			require.True(t, ok)
			require.Equal(t, "1", v.Str())

			v, ok = exporter.lastUpsertedSpan.Events().At(1).Attributes().Get("event2")
			require.True(t, ok)
			require.Equal(t, "2", v.Str())
		})
	})
	t.Run("test check span links attributes", func(t *testing.T) {
		assertSpanData(t, func(t *testing.T, exporter *loggingExporter) {
			mustPushTracesData(t, exporter, simpleTraces(1))

			v, ok := exporter.lastUpsertedSpan.Links().At(0).Attributes().Get("link1")
			require.True(t, ok)
			require.Equal(t, "3", v.Str())

			v, ok = exporter.lastUpsertedSpan.Links().At(1).Attributes().Get("link2")
			require.True(t, ok)
			require.Equal(t, "4", v.Str())
		})
	})
	t.Run("push data more than once success", func(t *testing.T) {
		exporter := newTestTracesExporter(t, defaultEndpoint)
		loggingExporter := newLoggingExporter(exporter)

		mustPushTracesData(t, loggingExporter, simpleTraces(1))
		mustPushTracesData(t, loggingExporter, simpleTraces(2))
		require.Equal(t, 3, loggingExporter.rows)
	})
}

func assertSpanData(t *testing.T, assert func(*testing.T, *loggingExporter)) {
	exporter := newTestTracesExporter(t, defaultEndpoint)
	loggingExporter := newLoggingExporter(exporter)

	assert(t, loggingExporter)
}

func newTestTracesExporter(t *testing.T, dsn string, fns ...func(*config.Config)) *Exporter {
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

func simpleTraces(count int) ptrace.Traces {
	traces := ptrace.NewTraces()
	rs := traces.ResourceSpans().AppendEmpty()
	rs.Resource().Attributes().PutStr(conventions.AttributeServiceName, "test-service")
	ss := rs.ScopeSpans().AppendEmpty()
	ss.Scope().SetName("io.opentelemetry.contrib.ydb")
	ss.Scope().SetVersion("1.0.0")
	ss.Scope().Attributes().PutStr("lib", "ydb")
	for i := 0; i < count; i++ {
		s := ss.Spans().AppendEmpty()
		now := time.Now()
		s.SetStartTimestamp(pcommon.NewTimestampFromTime(now))
		s.SetEndTimestamp(pcommon.NewTimestampFromTime(now.Add(time.Minute)))
		s.SetSpanID([8]byte{2, 3})
		s.SetTraceID([16]byte{4, 5})
		s.SetParentSpanID([8]byte{6, 7})
		s.SetName("span1")
		s.SetKind(1)
		s.Attributes().PutStr(conventions.AttributeServiceName, "span")

		event := s.Events().AppendEmpty()
		event.SetName("event1")
		event.Attributes().PutStr("event1", "1")

		event = s.Events().AppendEmpty()
		event.SetName("event2")
		event.Attributes().PutStr("event2", "2")

		link := s.Links().AppendEmpty()
		link.SetSpanID([8]byte{8, 9})
		link.SetTraceID([16]byte{10, 11})
		link.Attributes().PutStr("link1", "3")

		link = s.Links().AppendEmpty()
		link.SetSpanID([8]byte{12, 13})
		link.SetTraceID([16]byte{14, 15})
		link.Attributes().PutStr("link2", "4")
	}
	return traces
}

func mustPushTracesData(t *testing.T, exporter *loggingExporter, td ptrace.Traces) {
	err := exporter.pushData(context.TODO(), td)
	require.NoError(t, err)
}

type loggingExporter struct {
	exporter                 *Exporter
	rows                     int
	lastUpsertedResourceSpan ptrace.ResourceSpans
	lastUpsertedScopeSpan    ptrace.ScopeSpans
	lastUpsertedSpan         ptrace.Span
}

func newLoggingExporter(exporter *Exporter) *loggingExporter {
	return &loggingExporter{exporter: exporter}
}

func (l *loggingExporter) pushData(ctx context.Context, td ptrace.Traces) error {
	for i := 0; i < td.ResourceSpans().Len(); i++ {
		resourceSpan := td.ResourceSpans().At(i)
		for j := 0; j < resourceSpan.ScopeSpans().Len(); j++ {
			scopeSpans := resourceSpan.ScopeSpans().At(j)
			for k := 0; k < scopeSpans.Spans().Len(); k++ {
				span := scopeSpans.Spans().At(k)

				l.lastUpsertedResourceSpan = resourceSpan
				l.lastUpsertedScopeSpan = scopeSpans
				l.lastUpsertedSpan = span
				l.rows++
			}
		}
	}

	return l.exporter.PushData(ctx, td)
}
