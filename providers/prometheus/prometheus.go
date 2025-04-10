package prometheus

import (
	"strconv"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	vshardrouter "github.com/tarantool/go-vshard-router/v2"
)

// Check that provider implements MetricsProvider interface
var _ vshardrouter.MetricsProvider = (*Provider)(nil)

// Check that provider implements Collector interface
var _ prometheus.Collector = (*Provider)(nil)

// Provider is a struct that implements collector and provider methods.
// It gives users a simple way to use metrics for go-vshard-router.
type Provider struct {
	// cronDiscoveryEvent - histogram for cron discovery events.
	cronDiscoveryEvent *prometheus.HistogramVec
	// retryOnCall - counter for retry calls.
	retryOnCall *prometheus.CounterVec
	// requestDuration - histogram for map reduce and single request durations.
	requestDuration *prometheus.HistogramVec
}

// Describe sends the descriptors of each metric to the provided channel.
func (pp *Provider) Describe(ch chan<- *prometheus.Desc) {
	pp.cronDiscoveryEvent.Describe(ch)
	pp.retryOnCall.Describe(ch)
	pp.requestDuration.Describe(ch)
}

// Collect gathers the metrics and sends them to the provided channel.
func (pp *Provider) Collect(ch chan<- prometheus.Metric) {
	pp.cronDiscoveryEvent.Collect(ch)
	pp.retryOnCall.Collect(ch)
	pp.requestDuration.Collect(ch)
}

// CronDiscoveryEvent records the duration of a cron discovery event with labels.
func (pp *Provider) CronDiscoveryEvent(ok bool, duration time.Duration, reason string) {
	pp.cronDiscoveryEvent.With(prometheus.Labels{
		"ok":     strconv.FormatBool(ok),
		"reason": reason,
	}).Observe(float64(duration.Milliseconds()))
}

// RetryOnCall increments the retry counter for a specific reason.
func (pp *Provider) RetryOnCall(reason string) {
	pp.retryOnCall.With(prometheus.Labels{
		"reason": reason,
	}).Inc()
}

// RequestDuration records the duration of a request with labels for success and map-reduce usage.
func (pp *Provider) RequestDuration(duration time.Duration, procedure, rsName string, ok, mapReduce bool) {
	pp.requestDuration.With(prometheus.Labels{
		"ok":         strconv.FormatBool(ok),
		"map_reduce": strconv.FormatBool(mapReduce),
		"procedure":  procedure,
		"replicaset": rsName,
	}).Observe(float64(duration.Milliseconds()))
}

// NewPrometheusProvider - is an experimental function.
// Prometheus Provider is one of the ready-to-use providers implemented
// for go-vshard-router. It can be used to easily integrate metrics into
// your service without worrying about buckets, metric names, or other
// metric options.
//
// The provider implements both the interface required by vshard-router
// and the Prometheus collector interface.
//
// To register it in your Prometheus instance, use:
// registry.MustRegister(provider)
//
// Then, pass it to go-vshard-router so that it can manage the metrics:
//
//	vshard_router.NewRouter(ctx, vshard_router.Config{
//	    Metrics: provider,
//	})
//
// This approach simplifies the process of collecting and handling metrics,
// freeing you from manually managing metric-specific configurations.
func NewPrometheusProvider() *Provider {
	return &Provider{
		cronDiscoveryEvent: prometheus.NewHistogramVec(prometheus.HistogramOpts{
			Name:      "cron_discovery_event",
			Namespace: "vshard",
		}, []string{"ok", "reason"}), // Histogram for tracking cron discovery events

		retryOnCall: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name:      "retry_on_call",
			Namespace: "vshard",
		}, []string{"reason"}), // Counter for retry attempts

		requestDuration: prometheus.NewHistogramVec(prometheus.HistogramOpts{
			Name:      "request_duration",
			Namespace: "vshard",
		}, []string{"procedure", "ok", "map_reduce", "replicaset"}), // Histogram for request durations
	}
}
