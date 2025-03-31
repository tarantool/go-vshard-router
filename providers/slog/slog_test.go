package slog

import (
	"context"
	"log/slog"
	"os"
	"testing"

	"github.com/stretchr/testify/require"
	vshardrouter "github.com/tarantool/go-vshard-router/v2"
)

func TestNewSlogLogger(t *testing.T) {
	var slogProvider vshardrouter.LogfProvider

	require.NotPanics(t, func() {
		slogProvider = NewSlogLogger(nil)
	})

	require.Panics(t, func() {
		slogProvider.Warnf(context.TODO(), "")
	})
}

func TestSlogProvider(t *testing.T) {
	ctx := context.Background()

	// create new logger handler
	handler := slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{
		Level: slog.LevelError,
	})
	// create new SLogger instance
	sLogger := slog.New(handler)

	logProvider := NewSlogLogger(sLogger)

	require.NotPanics(t, func() {
		logProvider.Infof(ctx, "test %s", "s")
	})

	require.NotPanics(t, func() {
		logProvider.Warnf(ctx, "test %s", "s")
	})

	require.NotPanics(t, func() {
		logProvider.Errorf(ctx, "test %s", "s")
	})

	require.NotPanics(t, func() {
		logProvider.Debugf(ctx, "test %s", "s")
	})
}
