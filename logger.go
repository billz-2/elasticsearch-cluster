package esclient

import "context"

// Logger interface for debug/trace logging.
// Compatible with github.com/billz-2/packages/pkg/logger interface.
// If logger is not provided (nil), all logging is disabled (no-op).
type Logger interface {
	Debug(msg string, fields ...any)
	DebugWithCtx(ctx context.Context, msg string, fields ...any)
}

// noopLogger is a no-op implementation used when logger is not provided.
type noopLogger struct{}

func (noopLogger) Debug(msg string, fields ...any)                             {}
func (noopLogger) DebugWithCtx(ctx context.Context, msg string, fields ...any) {}

// safeLogger returns the provided logger or no-op logger if nil.
func safeLogger(log Logger) Logger {
	if log == nil {
		return noopLogger{}
	}
	return log
}
