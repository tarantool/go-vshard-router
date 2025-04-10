package prometheus

import (
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

func ExampleNewPrometheusProvider() {
	// Let's create new prometheus provider.
	provider := NewPrometheusProvider()

	// Create new prometheus registry.
	registry := prometheus.NewRegistry()
	// Register prometheus provider.
	registry.MustRegister(provider)

	// Create example http server.
	server := httptest.NewServer(promhttp.HandlerFor(registry, promhttp.HandlerOpts{}))
	defer server.Close()

	// Then we can register our provider in go vshard router.
	// It will use our provider to write prometheus metrics.
	/*
		vshard_router.NewRouter(ctx, vshard_router.Config{
			Metrics: provider,
		})
	*/

	provider.CronDiscoveryEvent(true, 150*time.Millisecond, "success")
	provider.RetryOnCall("timeout")
	provider.RequestDuration(200*time.Millisecond, "test", "test-rs", true, false)

	resp, err := http.Get(server.URL + "/metrics")
	if err != nil {
		panic(err)
	}

	defer resp.Body.Close()

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		panic(err)
	}

	metricsOutput := string(body)

	if strings.Contains(metricsOutput, "vshard_request_duration_bucket") {
		fmt.Println("Metrics output contains vshard_request_duration_bucket")
	}
	if strings.Contains(metricsOutput, "vshard_cron_discovery_event_bucket") {
		fmt.Println("Metrics output contains vshard_cron_discovery_event_bucket")
	}

	if strings.Contains(metricsOutput, "vshard_retry_on_call") {
		fmt.Println("Metrics output contains vshard_retry_on_call")
	}
	// Output: Metrics output contains vshard_request_duration_bucket
	// Metrics output contains vshard_cron_discovery_event_bucket
	// Metrics output contains vshard_retry_on_call
}
