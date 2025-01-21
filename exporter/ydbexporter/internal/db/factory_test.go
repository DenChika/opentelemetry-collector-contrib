package db

import (
	"github.com/open-telemetry/opentelemetry-collector-contrib/exporter/ydbexporter/internal/config"
	"github.com/stretchr/testify/assert"
	"go.uber.org/zap/zaptest"
	"testing"
)

const (
	defaultEndpoint = "grpcs://localhost:2135"
	defaultDatabase = "/local"
)

func TestDbFactory_buildDSN(t *testing.T) {
	type fields struct {
		Endpoint string
		Database string
	}
	tests := []struct {
		name    string
		fields  fields
		want    string
		wantErr error
	}{
		{
			name: "valid default config",
			fields: fields{
				Endpoint: defaultEndpoint,
				Database: defaultDatabase,
			},
			want: "grpcs://localhost:2135/local",
		},
		{
			name: "valid custom config",
			fields: fields{
				Endpoint: "grpcs://127.0.0.1:2135",
				Database: "/custom",
			},
			want: "grpcs://127.0.0.1:2135/custom",
		},
		{
			name: "grpc or grpcs required",
			fields: fields{
				Endpoint: "tcp://localhost:2135",
				Database: defaultDatabase,
			},
			wantErr: errOnlyGrpcSupport,
		},
		{
			name: "host is null",
			fields: fields{
				Endpoint: "grpcs://",
				Database: defaultDatabase,
			},
			wantErr: errEndpointIsNotSpecified,
		},
		{
			name: "path is null",
			fields: fields{
				Endpoint: defaultEndpoint,
				Database: "",
			},
			wantErr: errEndpointIsNotSpecified,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			logger := zaptest.NewLogger(t)
			cfg := &config.Config{
				Endpoint: tt.fields.Endpoint,
				Database: tt.fields.Database,
			}

			factory := NewFactory(cfg, logger)

			got, err := factory.buildDSN()

			if tt.wantErr != nil {
				assert.ErrorIs(t, err, tt.wantErr, "buildDSN()")
			} else {
				assert.Equal(t, tt.want, got, "buildDSN()")
			}
		})
	}
}

func TestDbFactory_buildDSN_failedParsing(t *testing.T) {
	type fields struct {
		Endpoint string
		Database string
	}
	tests := []struct {
		name   string
		fields fields
	}{
		{
			name:   "empty url",
			fields: fields{},
		},
		{
			name: "invalid characters",
			fields: fields{
				Endpoint: "local host:2135",
				Database: defaultDatabase,
			},
		},
		{
			name: "missing scheme",
			fields: fields{
				Endpoint: "localhost:2135",
				Database: defaultDatabase,
			},
		},
		{
			name: "missing host",
			fields: fields{
				Endpoint: ":2135",
				Database: defaultDatabase,
			},
		},
		{
			name: "missing port",
			fields: fields{
				Endpoint: "localhost",
				Database: defaultDatabase,
			},
		},
		{
			name: "invalid port",
			fields: fields{
				Endpoint: "localhost:abc",
				Database: defaultDatabase,
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			logger := zaptest.NewLogger(t)
			cfg := &config.Config{
				Endpoint: tt.fields.Endpoint,
				Database: tt.fields.Database,
			}

			factory := NewFactory(cfg, logger)

			_, err := factory.buildDSN()

			assert.Error(t, err)
		})
	}
}
