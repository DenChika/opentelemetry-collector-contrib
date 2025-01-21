// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package logs

import (
	"context"
	"os"
	"testing"
	"time"

	"github.com/open-telemetry/opentelemetry-collector-contrib/exporter/ydbexporter/internal/config"

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
	if os.Getenv("RUN_DOCKER_TESTS") == "" {
		t.Skip()
	}
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

func TestLogsExporter_createRecord(t *testing.T) {
	if os.Getenv("RUN_DOCKER_TESTS") == "" {
		t.Skip()
	}
	t.Run("create record success", func(t *testing.T) {
		exporter := newTestLogsExporter(t, defaultEndpoint)
		_, err := exporter.createRecord(plog.NewResourceLogs(), plog.NewLogRecord(), plog.NewScopeLogs())
		require.NoError(t, err)
	})
}

func TestLogsExporter_PushData(t *testing.T) {
	if os.Getenv("RUN_DOCKER_TESTS") == "" {
		t.Skip()
	}
	t.Run("push data success", func(t *testing.T) {
		exporter := newTestLogsExporter(t, defaultEndpoint)
		mustPushLogsData(t, exporter, simpleLogs(1))
		mustPushLogsData(t, exporter, simpleLogs(2))
	})
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
	rl.Resource().Attributes().PutStr("service.name", "test-service")
	sl := rl.ScopeLogs().AppendEmpty()
	sl.SetSchemaUrl("https://opentelemetry.io/schemas/1.7.0")
	sl.Scope().SetName("io.opentelemetry.contrib.ydb")
	sl.Scope().SetVersion("1.0.0")
	sl.Scope().Attributes().PutStr("lib", "ydb")
	for i := 0; i < count; i++ {
		r := sl.LogRecords().AppendEmpty()
		r.SetTimestamp(pcommon.NewTimestampFromTime(time.Now()))
		r.Attributes().PutStr(conventions.AttributeServiceName, "v")
	}
	return logs
}

func mustPushLogsData(t *testing.T, exporter *Exporter, ld plog.Logs) {
	err := exporter.PushData(context.TODO(), ld)
	require.NoError(t, err)
}
