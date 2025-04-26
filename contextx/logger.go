package contextx

import (
	"context"
	"log/slog"
)

type __contextx_logger__ struct {
}

func WithLogger(ctx context.Context, logger *slog.Logger) context.Context {
	return context.WithValue(ctx, __contextx_logger__{}, logger)
}

func Logger(ctx context.Context) *slog.Logger {
	val := ctx.Value(__contextx_logger__{})
	if val == nil {
		return slog.Default()
	}
	return val.(*slog.Logger)
}
