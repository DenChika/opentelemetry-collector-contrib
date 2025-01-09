// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package traces

import (
	"context"
	"github.com/open-telemetry/opentelemetry-collector-contrib/exporter/ydbexporter/internal/config"
	"go.opentelemetry.io/collector/pdata/ptrace"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/collector/pdata/pcommon"
	conventions "go.opentelemetry.io/collector/semconv/v1.18.0"
	"go.uber.org/zap/zaptest"
)

const defaultEndpoint = "grpc://localhost:2136"

func TestTracesExporter_New(t *testing.T) {
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

func TestTracesExporter_createRecord(t *testing.T) {
	t.Run("create record success", func(t *testing.T) {
		exporter := newTestTracesExporter(t, defaultEndpoint)
		_, err := exporter.createRecord(ptrace.NewResourceSpans(), ptrace.NewScopeSpans(), ptrace.NewSpan())
		require.NoError(t, err)
	})
}

func TestTracesExporter_PushData(t *testing.T) {
	t.Run("push data success", func(t *testing.T) {
		exporter := newTestTracesExporter(t, defaultEndpoint)
		mustPushTracesData(t, exporter, simpleTraces(1))
		mustPushTracesData(t, exporter, simpleTraces(2))
	})
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
	rs.SetSchemaUrl("https://opentelemetry.io/schemas/1.4.0")
	rs.Resource().SetDroppedAttributesCount(10)
	rs.Resource().Attributes().PutStr("service.name", "test-service")
	ss := rs.ScopeSpans().AppendEmpty()
	ss.Scope().SetName("io.opentelemetry.contrib.ydb")
	ss.Scope().SetVersion("1.0.0")
	ss.SetSchemaUrl("https://opentelemetry.io/schemas/1.7.0")
	ss.Scope().SetDroppedAttributesCount(20)
	ss.Scope().Attributes().PutStr("lib", "ydb")
	for i := 0; i < count; i++ {
		s := ss.Spans().AppendEmpty()
		s.SetStartTimestamp(pcommon.NewTimestampFromTime(time.Now()))
		s.SetEndTimestamp(pcommon.NewTimestampFromTime(time.Now()))
		s.Attributes().PutStr(conventions.AttributeServiceName, "v")
		event := s.Events().AppendEmpty()
		event.SetName("event1")
		event.SetTimestamp(pcommon.NewTimestampFromTime(time.Now()))
		link := s.Links().AppendEmpty()
		link.Attributes().PutStr("k", "v")
	}
	return traces
}

func mustPushTracesData(t *testing.T, exporter *Exporter, td ptrace.Traces) {
	err := exporter.PushData(context.TODO(), td)
	require.NoError(t, err)
}
