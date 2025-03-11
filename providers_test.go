package vshard_router_test

import (
	"context"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"os"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"github.com/stretchr/testify/require"

	vshardrouter "github.com/tarantool/go-vshard-router/v2"
)

var (
	emptyMetrics = vshardrouter.EmptyMetrics{}
	stdoutLogger = vshardrouter.StdoutLoggerf{}
)

func TestEmptyMetrics_RetryOnCall(t *testing.T) {
	require.NotPanics(t, func() {
		emptyMetrics.RetryOnCall("")
	})
}

func TestEmptyMetrics_RequestDuration(t *testing.T) {
	require.NotPanics(t, func() {
		emptyMetrics.RequestDuration(time.Second, "test", false, false)
	})
}

func TestEmptyMetrics_CronDiscoveryEvent(t *testing.T) {
	require.NotPanics(t, func() {
		emptyMetrics.CronDiscoveryEvent(false, time.Second, "")
	})
}

func TestStdoutLogger(t *testing.T) {
	ctx := context.TODO()

	require.NotPanics(t, func() {
		stdoutLogger.Errorf(ctx, "")
	})
	require.NotPanics(t, func() {
		stdoutLogger.Infof(ctx, "")
	})
	require.NotPanics(t, func() {
		stdoutLogger.Warnf(ctx, "")
	})
	require.NotPanics(t, func() {
		stdoutLogger.Debugf(ctx, "")
	})
}

func TestNewSlogLogger(t *testing.T) {
	var slogProvider vshardrouter.LogfProvider

	require.NotPanics(t, func() {
		slogProvider = vshardrouter.NewSlogLogger(nil)
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

	logProvider := vshardrouter.NewSlogLogger(sLogger)

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

func TestPrometheusMetricsServer(t *testing.T) {
	provider := vshardrouter.NewPrometheusProvider()

	registry := prometheus.NewRegistry()
	registry.MustRegister(provider)

	server := httptest.NewServer(promhttp.HandlerFor(registry, promhttp.HandlerOpts{}))
	defer server.Close()

	provider.CronDiscoveryEvent(true, 150*time.Millisecond, "success")
	provider.RetryOnCall("timeout")
	provider.RequestDuration(200*time.Millisecond, "test", true, false)
	provider.DecodeDuration(200*time.Millisecond, true)

	resp, err := http.Get(server.URL + "/metrics")
	require.NoError(t, err)

	defer resp.Body.Close()

	body, err := io.ReadAll(resp.Body)
	require.NoError(t, err)
	metricsOutput := string(body)

	require.Contains(t, metricsOutput, "vshard_request_duration_bucket")
	require.Contains(t, metricsOutput, "vshard_cron_discovery_event_bucket")
	require.Contains(t, metricsOutput, "vshard_retry_on_call")
	require.Contains(t, metricsOutput, "decode_duration")
}
