package ydbexporter

import (
	"context"
	"github.com/open-telemetry/opentelemetry-collector-contrib/exporter/ydbexporter/internal/config"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/collector/component/componenttest"
	"go.opentelemetry.io/collector/exporter/exportertest"
	"os"
	"testing"
)

const authType = "anonymous"

func TestCreateDefaultConfig(t *testing.T) {
	factory := NewFactory()
	cfg := factory.CreateDefaultConfig()
	assert.NotNil(t, cfg, "failed to create default config")
	assert.NoError(t, componenttest.CheckConfigStruct(cfg))
}

func TestFactory_CreateLogsExporter(t *testing.T) {
	if os.Getenv("RUN_DOCKER_TESTS") == "" {
		t.Skip()
	}
	factory := NewFactory()
	params := exportertest.NewNopCreateSettings()
	cfg := config.WithDefaultConfig(func(c *config.Config) {
		c.AuthType = authType
	})

	exporter, err := factory.CreateLogsExporter(context.Background(), params, cfg)

	require.NoError(t, err)
	require.NotNil(t, exporter)
	require.NoError(t, exporter.Shutdown(context.TODO()))
}

func TestFactory_CreateTracesExporter(t *testing.T) {
	if os.Getenv("RUN_DOCKER_TESTS") == "" {
		t.Skip()
	}
	factory := NewFactory()
	params := exportertest.NewNopCreateSettings()
	cfg := config.WithDefaultConfig(func(c *config.Config) {
		c.AuthType = authType
	})

	exporter, err := factory.CreateTracesExporter(context.Background(), params, cfg)

	require.NoError(t, err)
	require.NotNil(t, exporter)
	require.NoError(t, exporter.Shutdown(context.TODO()))
}

func TestFactory_CreateMetricsExporter(t *testing.T) {
	if os.Getenv("RUN_DOCKER_TESTS") == "" {
		t.Skip()
	}
	factory := NewFactory()
	params := exportertest.NewNopCreateSettings()
	cfg := config.WithDefaultConfig(func(c *config.Config) {
		c.AuthType = authType
	})

	exporter, err := factory.CreateMetricsExporter(context.Background(), params, cfg)

	require.NoError(t, err)
	require.NotNil(t, exporter)
	require.NoError(t, exporter.Shutdown(context.TODO()))
}
