package prometheus

import (
	"io"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"github.com/stretchr/testify/require"
)

func TestPrometheusMetricsServer(t *testing.T) {
	provider := NewPrometheusProvider()

	registry := prometheus.NewRegistry()
	registry.MustRegister(provider)

	server := httptest.NewServer(promhttp.HandlerFor(registry, promhttp.HandlerOpts{}))
	defer server.Close()

	provider.CronDiscoveryEvent(true, 150*time.Millisecond, "success")
	provider.RetryOnCall("timeout")
	provider.RequestDuration(200*time.Millisecond, "test", true, false)

	resp, err := http.Get(server.URL + "/metrics")
	require.NoError(t, err)

	defer resp.Body.Close()

	body, err := io.ReadAll(resp.Body)
	require.NoError(t, err)
	metricsOutput := string(body)

	require.Contains(t, metricsOutput, "vshard_request_duration_bucket")
	require.Contains(t, metricsOutput, "vshard_cron_discovery_event_bucket")
	require.Contains(t, metricsOutput, "vshard_retry_on_call")
}
