package vshard_router //nolint:revive

import (
	"context"
	"fmt"
	"log"
	"log/slog"
	"strconv"
	"time"

	"github.com/prometheus/client_golang/prometheus"
)

var (
	emptyMetricsProvider MetricsProvider = (*EmptyMetrics)(nil)
	emptyLogfProvider    LogfProvider    = emptyLogger{}

	// Ensure StdoutLoggerf implements LogfProvider
	_ LogfProvider = StdoutLoggerf{}
	// Ensure SlogLoggerf implements LogfProvider
	_ LogfProvider = &SlogLoggerf{}
)

// LogfProvider an interface to inject a custom logger.
type LogfProvider interface {
	Debugf(ctx context.Context, format string, v ...any)
	Infof(ctx context.Context, format string, v ...any)
	Warnf(ctx context.Context, format string, v ...any)
	Errorf(ctx context.Context, format string, v ...any)
}

type emptyLogger struct{}

func (e emptyLogger) Debugf(_ context.Context, _ string, _ ...any) {}
func (e emptyLogger) Infof(_ context.Context, _ string, _ ...any)  {}
func (e emptyLogger) Warnf(_ context.Context, _ string, _ ...any)  {}
func (e emptyLogger) Errorf(_ context.Context, _ string, _ ...any) {}

// StdoutLogLevel is a type to control log level for StdoutLoggerf.
type StdoutLogLevel int

const (
	// StdoutLogDefault is equal to default value of StdoutLogLevel. Acts like StdoutLogInfo.
	StdoutLogDefault StdoutLogLevel = iota
	// StdoutLogDebug enables debug or higher level logs for StdoutLoggerf
	StdoutLogDebug
	// StdoutLogInfo enables only info or higher level logs for StdoutLoggerf
	StdoutLogInfo
	// StdoutLogWarn enables only warn or higher level logs for StdoutLoggerf
	StdoutLogWarn
	// StdoutLogError enables error level logs for StdoutLoggerf
	StdoutLogError
)

// StdoutLoggerf a logger that prints into stderr
type StdoutLoggerf struct {
	// LogLevel controls log level to print, see StdoutLogLevel constants for details.
	LogLevel StdoutLogLevel
}

func (s StdoutLoggerf) printLevel(level StdoutLogLevel, prefix string, format string, v ...any) {
	var currentLogLevel = s.LogLevel

	if currentLogLevel == StdoutLogDefault {
		currentLogLevel = StdoutLogInfo
	}

	if level >= currentLogLevel {
		log.Printf(prefix+format, v...)
	}
}

// Debugf implements Debugf method for LogfProvider interface
func (s StdoutLoggerf) Debugf(_ context.Context, format string, v ...any) {
	s.printLevel(StdoutLogDebug, "[DEBUG] ", format, v...)
}

// Infof implements Infof method for LogfProvider interface
func (s StdoutLoggerf) Infof(_ context.Context, format string, v ...any) {
	s.printLevel(StdoutLogInfo, "[INFO] ", format, v...)
}

// Warnf implements Warnf method for LogfProvider interface
func (s StdoutLoggerf) Warnf(_ context.Context, format string, v ...any) {
	s.printLevel(StdoutLogWarn, "[WARN] ", format, v...)
}

// Errorf implements Errorf method for LogfProvider interface
func (s StdoutLoggerf) Errorf(_ context.Context, format string, v ...any) {
	s.printLevel(StdoutLogError, "[ERROR] ", format, v...)
}

// NewSlogLogger wraps slog logger
func NewSlogLogger(logger *slog.Logger) LogfProvider {
	return &SlogLoggerf{
		Logger: logger,
	}
}

// SlogLoggerf is adapter for slog to Logger interface.
type SlogLoggerf struct {
	Logger *slog.Logger
}

// Debugf implements Debugf method for LogfProvider interface
func (s *SlogLoggerf) Debugf(ctx context.Context, format string, v ...any) {
	s.Logger.DebugContext(ctx, fmt.Sprintf(format, v...))
}

// Infof implements Infof method for LogfProvider interface
func (s SlogLoggerf) Infof(ctx context.Context, format string, v ...any) {
	s.Logger.InfoContext(ctx, fmt.Sprintf(format, v...))
}

// Warnf implements Warnf method for LogfProvider interface
func (s SlogLoggerf) Warnf(ctx context.Context, format string, v ...any) {
	s.Logger.WarnContext(ctx, fmt.Sprintf(format, v...))
}

// Errorf implements Errorf method for LogfProvider interface
func (s SlogLoggerf) Errorf(ctx context.Context, format string, v ...any) {
	s.Logger.ErrorContext(ctx, fmt.Sprintf(format, v...))
}

// Metrics

// MetricsProvider is an interface for passing library metrics to your prometheus/graphite and other metrics.
// This logic is experimental and may be changed in the release.
type MetricsProvider interface {
	CronDiscoveryEvent(ok bool, duration time.Duration, reason string)
	RetryOnCall(reason string)
	RequestDuration(duration time.Duration, procedure string, ok, mapReduce bool)
	DecodeDuration(duration time.Duration, mapReduce bool)
}

// EmptyMetrics is default empty metrics provider
// you can embed this type and realize just some metrics
type EmptyMetrics struct{}

func (e *EmptyMetrics) CronDiscoveryEvent(_ bool, _ time.Duration, _ string)  {}
func (e *EmptyMetrics) RetryOnCall(_ string)                                  {}
func (e *EmptyMetrics) RequestDuration(_ time.Duration, _ string, _, _ bool)  {}
func (e *EmptyMetrics) DecodeDuration(duration time.Duration, mapReduce bool) {}

// Prometheus provider

// DefaultPrometheusProvider is an experimental interface that implements a collector
// and provider for go-vshard metrics.
type DefaultPrometheusProvider interface {
	prometheus.Collector
	MetricsProvider
}

// prometheusProvider is a struct that implements DefaultPrometheusProvider.
type prometheusProvider struct {
	// cronDiscoveryEvent - histogram for cron discovery events.
	cronDiscoveryEvent *prometheus.HistogramVec
	// retryOnCall - counter for retry calls.
	retryOnCall *prometheus.CounterVec
	// requestDuration - histogram for map reduce and single request durations.
	requestDuration *prometheus.HistogramVec
	// decodeDuration - histogram for decode msgpack durations.
	decodeDuration *prometheus.HistogramVec
}

// Describe sends the descriptors of each metric to the provided channel.
func (pp *prometheusProvider) Describe(ch chan<- *prometheus.Desc) {
	pp.cronDiscoveryEvent.Describe(ch)
	pp.retryOnCall.Describe(ch)
	pp.requestDuration.Describe(ch)
	pp.decodeDuration.Describe(ch)
}

// Collect gathers the metrics and sends them to the provided channel.
func (pp *prometheusProvider) Collect(ch chan<- prometheus.Metric) {
	pp.cronDiscoveryEvent.Collect(ch)
	pp.retryOnCall.Collect(ch)
	pp.requestDuration.Collect(ch)
	pp.decodeDuration.Collect(ch)
}

// CronDiscoveryEvent records the duration of a cron discovery event with labels.
func (pp *prometheusProvider) CronDiscoveryEvent(ok bool, duration time.Duration, reason string) {
	pp.cronDiscoveryEvent.With(prometheus.Labels{
		"ok":     strconv.FormatBool(ok),
		"reason": reason,
	}).Observe(float64(duration.Milliseconds()))
}

// RetryOnCall increments the retry counter for a specific reason.
func (pp *prometheusProvider) RetryOnCall(reason string) {
	pp.retryOnCall.With(prometheus.Labels{
		"reason": reason,
	}).Inc()
}

// RequestDuration records the duration of a request with labels for success and map-reduce usage.
func (pp *prometheusProvider) RequestDuration(duration time.Duration, procedure string, ok, mapReduce bool) {
	pp.requestDuration.With(prometheus.Labels{
		"ok":         strconv.FormatBool(ok),
		"map_reduce": strconv.FormatBool(mapReduce),
		"procedure":  procedure,
	}).Observe(float64(duration.Milliseconds()))
}

func (pp *prometheusProvider) DecodeDuration(duration time.Duration, mapReduce bool) {
	pp.decodeDuration.With(prometheus.Labels{
		"map_reduce": strconv.FormatBool(mapReduce),
	}).Observe(float64(duration.Milliseconds()))
}

// NewPrometheusProvider - is an experimental function.
// It returns a standard provider, which you can later register in prometheus and as a metrics provider.
func NewPrometheusProvider() DefaultPrometheusProvider {
	return &prometheusProvider{
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
		}, []string{"procedure", "ok", "map_reduce"}), // Histogram for request durations
		decodeDuration: prometheus.NewHistogramVec(prometheus.HistogramOpts{
			Name:      "decode_duration",
			Namespace: "vshard",
		}, []string{"map_reduce"}),
	}
}

// TopologyProvider is external module that can lookup current topology of cluster
// it might be etcd/config/consul or smth else
type TopologyProvider interface {
	// Init should create the current topology at the beginning
	// and change the state during the process of changing the point of receiving the cluster configuration
	Init(t TopologyController) error
	// Close closes all connections if the provider created them
	Close()
}
