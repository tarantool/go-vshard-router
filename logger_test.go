package vshard_router //nolint:revive

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"testing"

	"github.com/stretchr/testify/require"
)

type recordedLogf struct {
	level   slog.Level
	message string
	ctx     context.Context
	calls   int
}

func (l *recordedLogf) record(ctx context.Context, level slog.Level, format string, args ...any) {
	l.level, l.message, l.ctx = level, fmt.Sprintf(format, args...), ctx
	l.calls++
}
func (l *recordedLogf) Debugf(ctx context.Context, f string, a ...any) {
	l.record(ctx, slog.LevelDebug, f, a...)
}
func (l *recordedLogf) Infof(ctx context.Context, f string, a ...any) {
	l.record(ctx, slog.LevelInfo, f, a...)
}
func (l *recordedLogf) Warnf(ctx context.Context, f string, a ...any) {
	l.record(ctx, slog.LevelWarn, f, a...)
}
func (l *recordedLogf) Errorf(ctx context.Context, f string, a ...any) {
	l.record(ctx, slog.LevelError, f, a...)
}

func TestSlogHandler(t *testing.T) {
	for _, tc := range []struct {
		slogLevel slog.Level
		provider  slog.Level
	}{
		{slog.LevelDebug, slog.LevelDebug},
		{slog.LevelInfo, slog.LevelInfo},
		{slog.LevelWarn, slog.LevelError},
		{slog.LevelError, slog.LevelError},
	} {
		t.Run(tc.slogLevel.String(), func(t *testing.T) {
			provider := &recordedLogf{}
			logger := slog.New(tarantoolLogHandler{loggerf: provider})
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			logger.With("address", "127.0.0.1:3301").WithGroup("tarantool").Log(ctx, tc.slogLevel,
				"reconnect failed", "attempt", 3, "error", errors.New("connection refused"))
			require.Equal(t, 1, provider.calls)
			require.Equal(t, tc.provider, provider.level)
			require.Same(t, ctx, provider.ctx)
			require.Contains(t, provider.message, "tarantool: reconnect failed")
			require.Contains(t, provider.message, `address="127.0.0.1:3301"`)
			require.Contains(t, provider.message, "tarantool.attempt=3")
			require.Contains(t, provider.message, `tarantool.error="connection refused"`)
		})
	}
}

func TestSlogHandlerGroups(t *testing.T) {
	provider := &recordedLogf{}
	base := slog.New(tarantoolLogHandler{loggerf: provider})
	child := base.WithGroup("tarantool").With("attempt", 3).WithGroup("watch")
	child.Warn("watch failed", slog.Group("details", "address", "localhost"))
	require.Contains(t, provider.message, "tarantool.attempt=3")
	require.Contains(t, provider.message, `tarantool.watch.details.address="localhost"`)
	base.Warn("base message")
	require.Equal(t, "tarantool: base message", provider.message)
}
