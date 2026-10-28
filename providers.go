package vshard_router //nolint:revive

import (
	"log/slog"
	"time"
)

var (
	emptyMetricsProvider MetricsProvider = (*EmptyMetrics)(nil)
	// discardLogger drops everything. It is what an unconfigured router uses,
	// matching go-tarantool, which discards logs when no logger is set.
	discardLogger = slog.New(slog.DiscardHandler)
)

// Metrics

// MetricsProvider is an interface for passing library metrics to your prometheus/graphite and other metrics.
// This logic is experimental and may be changed in the release.
type MetricsProvider interface {
	CronDiscoveryEvent(ok bool, duration time.Duration, reason string)
	RetryOnCall(reason string)
	RequestDuration(duration time.Duration, procedure string, ok, mapReduce bool)
}

// EmptyMetrics is default empty metrics provider
// you can embed this type and realize just some metrics
type EmptyMetrics struct{}

func (e *EmptyMetrics) CronDiscoveryEvent(_ bool, _ time.Duration, _ string) {}
func (e *EmptyMetrics) RetryOnCall(_ string)                                 {}
func (e *EmptyMetrics) RequestDuration(_ time.Duration, _ string, _, _ bool) {}

// TopologyProvider is external module that can lookup current topology of cluster
// it might be etcd/config/consul or smth else
type TopologyProvider interface {
	// Init should create the current topology at the beginning
	// and change the state during the process of changing the point of receiving the cluster configuration
	Init(t TopologyController) error
	// Close closes all connections if the provider created them
	Close()
}
