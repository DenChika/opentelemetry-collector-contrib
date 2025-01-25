package traces

import (
	"context"
	"go.opentelemetry.io/collector/pdata/ptrace"
)

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

func (l *loggingExporter) createTable(ctx context.Context) error {
	return l.exporter.createTable(ctx)
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
