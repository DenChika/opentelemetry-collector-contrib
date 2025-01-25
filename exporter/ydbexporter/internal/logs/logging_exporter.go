package logs

import (
	"context"
	"go.opentelemetry.io/collector/pdata/plog"
)

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

func (l *loggingExporter) createTable(ctx context.Context) error {
	return l.exporter.createTable(ctx)
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
