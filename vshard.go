package vshard_router //nolint:revive

import (
	"context"
	"fmt"
	"log/slog"
	"sort"
	"sync/atomic"
	"time"

	"github.com/google/uuid"
	"github.com/snksoft/crc"
	"github.com/vmihailenco/msgpack/v5"

	tarantool "github.com/tarantool/go-tarantool/v3"
	"github.com/tarantool/go-tarantool/v3/pool"
)

var (
	// ErrInvalidConfig is returned when the configuration is invalid.
	ErrInvalidConfig = fmt.Errorf("config invalid")
	// ErrInvalidInstanceInfo is returned when the instance information is invalid.
	ErrInvalidInstanceInfo = fmt.Errorf("invalid instance info")
	// ErrInvalidReplicasetInfo is returned when the replicaset information is invalid.
	ErrInvalidReplicasetInfo = fmt.Errorf("invalid replicaset info")
	// ErrTopologyProvider is returned when there is an error from the topology provider.
	ErrTopologyProvider = fmt.Errorf("got error from topology provider")
)

type Router struct {
	cfg Config

	// nameToReplicaset is an atomic pointer, that points to an immutable object by our convention.
	// Whenever we add or remove a replicaset, we create a new map object.
	// Object under nameToReplicaset can be replaced to a new one only by TopologyController methods.
	// Assuming that we rarely add or remove some replicaset,
	// it should be the simplest and most efficient way of handling concurrent access.
	// Additionally, we can safely iterate over a map because it never changes.
	nameToReplicaset atomic.Pointer[map[string]*Replicaset]

	// The interface value is immutable after publication; cache entries are
	// updated through its concurrency-safe methods. RouteMapClean publishes a new cache.
	routeMap atomic.Pointer[routeMap]

	// refID is used for Map-Reduce operation. Since it must be unique (within connection) for each ref request,
	// we made it global and monotonically growing for each Router instance.
	refID atomic.Int64

	cancelDiscovery func()
}

func (r *Router) metrics() MetricsProvider {
	return r.cfg.Metrics
}

func (r *Router) log() *slog.Logger {
	// NewRouter always sets a logger via prepareCfg, but a Router built by hand
	// (as tests do) would otherwise panic on the first log call.
	if r.cfg.Logger == nil {
		return discardLogger
	}

	return r.cfg.Logger
}

func (r *Router) getRouteMap() routeMap {
	return *r.routeMap.Load()
}

func (r *Router) setRouteMap(routes routeMap) {
	r.routeMap.Store(&routes)
}

type Config struct {
	// Providers
	// Logger injects a logger. By default logs are discarded.
	// It is passed to the connection pools as well, so router and connector
	// events share one destination.
	Logger           *slog.Logger     // Logger is not required
	Metrics          MetricsProvider  // Metrics is not required
	TopologyProvider TopologyProvider // TopologyProvider is required provider

	// Discovery
	// DiscoveryTimeout is timeout between cron discovery job; by default there is no timeout.
	DiscoveryTimeout time.Duration
	DiscoveryMode    DiscoveryMode
	// DiscoveryWorkStep is a pause between calling buckets_discovery on storage
	// in buckets discovering logic. Default is 10ms.
	DiscoveryWorkStep time.Duration

	// BucketsSearchMode defines policy for Router.Route method.
	// Default value is BucketsSearchLegacy.
	// See BucketsSearchMode constants for more detail.
	BucketsSearchMode BucketsSearchMode

	// TotalBucketCount is the number of buckets in the cluster, not the largest
	// bucket ID. It is also the upper ID bound unless SparseBucketIDs is enabled.
	TotalBucketCount uint64
	// SparseBucketIDs enables a map-based route cache for uint64 bucket IDs in
	// 1..99_999_999_999_999, the range vshard accepts as Lua numbers rather than
	// cdata. Memory usage depends on the number of cached routes,
	// not the largest ID. Bucket IDs must be calculated by the application and
	// buckets must be bootstrapped outside the router in this mode.
	SparseBucketIDs bool
	User            string
	Password        string

	// ConnOpts configures each storage connection inside a replicaset pool.
	// Only the settings the v3 pool leaves to the connection are exposed here:
	// reconnection and per-connection events are the pool's business, and
	// logging goes through Logger, so Reconnect, MaxReconnects, Notify and
	// Logger are deliberately absent rather than silently ignored.
	ConnTimeout      time.Duration
	ConnRateLimit    uint
	ConnRLimitAction tarantool.RLimitAction
	ConnConcurrency  uint32
	ConnSkipSchema   bool
	ConnHandle       any
	ConnAllocator    tarantool.Allocator
}

type BucketStatInfo struct {
	BucketID uint64 `msgpack:"id"`
	Status   string `msgpack:"status"`
}

// tnt vshard storage returns map with 'int' keys for bucketStatInfo,
// example: map[id:48 status:active 1:48 2:active].
// But msgpackv5 supports only string keys when decoding maps into structs,
// see issue: https://github.com/vmihailenco/msgpack/issues/372
// To workaround this we decode BucketStatInfo manually.
// When the issue above will be resolved, this code can be (and should be) deleted.
func (bsi *BucketStatInfo) DecodeMsgpack(d *msgpack.Decoder) error {
	nKeys, err := d.DecodeMapLen()
	if err != nil {
		return err
	}

	for i := 0; i < nKeys; i++ {
		key, err := d.DecodeInterface()
		if err != nil {
			return err
		}

		keyName, _ := key.(string)
		switch keyName {
		case "id":
			if err := d.Decode(&bsi.BucketID); err != nil {
				return err
			}
		case "status":
			if err := d.Decode(&bsi.Status); err != nil {
				return err
			}
		default:
			// skip unused value
			if err := d.Skip(); err != nil {
				return err
			}
		}

	}

	return nil
}

// InstanceInfo represents the information about an instance.
// This struct holds the necessary details such as the name, address, and UUID of the instance.
type InstanceInfo struct {
	// Name is a required field for the instance.
	// Starting with Tarantool 3.0, this definition is made into a human-readable name,
	// and it is now mandatory for use in the library.
	// The Name should be a unique identifier for the instance.
	Name string

	// Addr specifies the address of the instance.
	// This can be an IP address or a domain name, depending on how the instance is accessed.
	// It is necessary for connecting to the instance or identifying its location.
	Addr string

	// UUID is an optional field that provides a globally unique identifier (UUID) for the instance.
	// While this information is not mandatory, it can be useful for internal management or tracking purposes.
	// The UUID ensures that each instance can be identified uniquely, but it is not required for basic operations.
	UUID uuid.UUID

	// Dialer allows to use a custom dialer instead of the default one (tarantool.NetDialer).
	// This parameter is temporarily optional and will become mandatory in the future.
	Dialer tarantool.Dialer
}

func (ii InstanceInfo) String() string {
	if ii.UUID == uuid.Nil {
		return fmt.Sprintf("{name: %s, addr: %s}", ii.Name, ii.Addr)
	}

	return fmt.Sprintf("{name: %s, uuid: %s, addr: %s}", ii.Name, ii.UUID, ii.Addr)
}

func (ii InstanceInfo) Validate() error {
	if ii.Name == "" {
		return fmt.Errorf("%w: empty name", ErrInvalidInstanceInfo)
	}

	if ii.Addr == "" {
		return fmt.Errorf("%w: empty addr", ErrInvalidInstanceInfo)
	}

	return nil
}

// --------------------------------------------------------------------------------
// -- Configuration
// --------------------------------------------------------------------------------

// NewRouter - the main library function.
// It creates a Router for direct request routing in a sharded Tarantool cluster.
// It connects to masters and replicas for load distribution and fault tolerance.
func NewRouter(ctx context.Context, cfg Config) (*Router, error) {
	var err error

	cfg, err = prepareCfg(cfg)
	if err != nil {
		return nil, err
	}

	router := &Router{
		cfg: cfg,
	}
	router.setEmptyRouteMap()
	router.setEmptyNameToReplicaset()

	err = cfg.TopologyProvider.Init(router.Topology())
	if err != nil {
		router.log().ErrorContext(ctx, "Can't create new topology provider", "error", err)

		return nil, fmt.Errorf("%w; cant init topology with err: %w", ErrTopologyProvider, err)
	}

	err = router.DiscoveryAllBuckets(ctx)
	if err != nil {
		router.log().ErrorContext(ctx, "router.DiscoveryAllBuckets failed", "error", err)
	}

	if cfg.DiscoveryMode == DiscoveryModeOn {
		//nolint:gosec
		discoveryCronCtx, cancelFunc := context.WithCancel(ctx)

		// run background cron discovery loop
		// suppress linter warning: Non-inherited new context, use function like `context.WithXXX` instead (contextcheck)
		//nolint:contextcheck
		go router.cronDiscovery(discoveryCronCtx)

		router.cancelDiscovery = cancelFunc
	}

	return router, nil
}

// BucketSet Set a bucket to a replicaset.
func (r *Router) BucketSet(bucketID uint64, rsName string) (*Replicaset, error) {
	routeMap := r.getRouteMap()
	maxBucketID := routeMap.maxBucketID()
	if !isValidBucketID(bucketID, maxBucketID) {
		return nil, fmt.Errorf("bucket id is out of range: %d (valid range 1..%d)", bucketID, maxBucketID)
	}

	nameToReplicasetRef := r.getNameToReplicaset()

	rs := nameToReplicasetRef[rsName]
	if rs == nil {
		return nil, newVShardErrorNoRouteToBucket(bucketID)
	}

	routeMap.store(bucketID, rs)

	return rs, nil
}

func (r *Router) BucketReset(bucketID uint64) {
	routeMap := r.getRouteMap()
	if !isValidBucketID(bucketID, routeMap.maxBucketID()) {
		return
	}

	routeMap.store(bucketID, nil)
}

func (r *Router) RouteMapClean() {
	r.setEmptyRouteMap()
}

func (r *Router) setEmptyRouteMap() {
	var routes routeMap
	if r.cfg.SparseBucketIDs {
		routes = &sparseRouteMap{}
	} else {
		routes = &denseRouteMap{
			buckets: make([]atomic.Pointer[Replicaset], r.cfg.TotalBucketCount+1),
		}
	}
	r.setRouteMap(routes)
}

func prepareCfg(cfg Config) (Config, error) {
	const discoveryTimeoutDefault = 1 * time.Minute
	const discoveryWorkStepDefault = 10 * time.Millisecond

	err := validateCfg(cfg)
	if err != nil {
		return Config{}, fmt.Errorf("%v: %v", ErrInvalidConfig, err)
	}

	if cfg.DiscoveryTimeout == 0 {
		cfg.DiscoveryTimeout = discoveryTimeoutDefault
	}

	if cfg.Logger == nil {
		cfg.Logger = discardLogger
	}

	if cfg.Metrics == nil {
		cfg.Metrics = emptyMetricsProvider
	}

	if cfg.DiscoveryWorkStep == 0 {
		cfg.DiscoveryWorkStep = discoveryWorkStepDefault
	}

	return cfg, nil
}

func validateCfg(cfg Config) error {
	if cfg.TopologyProvider == nil {
		return fmt.Errorf("topology provider is nil")
	}

	if cfg.TotalBucketCount == 0 {
		return fmt.Errorf("bucket count must be greater than 0")
	}

	// The v3 pool manages reconnection itself and rejects Reconnect,
	// MaxReconnects and Notify per instance. Config does not expose them, so
	// there is nothing to validate here.

	return nil
}

// --------------------------------------------------------------------------------
// -- Other
// --------------------------------------------------------------------------------

var h = crc.NewHash(&crc.Parameters{
	Width:      32,
	Polynomial: 0x1EDC6F41,
	FinalXor:   0x0,
	ReflectIn:  true,
	ReflectOut: true,
	Init:       0xFFFFFFFF,
})

func BucketIDStrCRC32(shardKey string, totalBucketCount uint64) uint64 {
	return h.CalculateCRC([]byte(shardKey))%totalBucketCount + 1
}

// BucketIDStrCRC32 return the bucket identifier from the parameter used for sharding.
func (r *Router) BucketIDStrCRC32(shardKey string) uint64 {
	return BucketIDStrCRC32(shardKey, r.cfg.TotalBucketCount)
}

// BucketCount returns the total number of buckets specified in cfg.
func (r *Router) BucketCount() uint64 {
	return r.cfg.TotalBucketCount
}

// -------------------------------------------------------------------------------_
// -- Bootstrap
// --------------------------------------------------------------------------------

// ClusterBootstrap distributes buckets across replicasets.
// Idempotent: if any of replicasets has buckets, the cluster is considered already bootstrapped; function
// returns a NON_EMPTY error, or succeeds without changes when ifNotBootstrapped is true.
// With ifNotBootstrapped, it returns an error if the replicasets hold fewer than TotalBucketCount
// buckets in total: an earlier bootstrap stopped halfway and has to be finished manually.
//
// Buckets are split according to ReplicasetInfo.Weight. If no replicaset has a weight set,
// all of them get equal shares, as vshard's default weight of 1 does.
func (r *Router) ClusterBootstrap(ctx context.Context, ifNotBootstrapped bool) error {
	if r.cfg.SparseBucketIDs {
		return fmt.Errorf("cluster bootstrap is not supported for sparse bucket IDs")
	}

	nameToReplicasetRef := r.getNameToReplicaset()

	if len(nameToReplicasetRef) == 0 {
		return fmt.Errorf("no replicasets to bootstrap")
	}

	// Check that every replicaset is empty before bootstrapping. If at least one
	// replicaset already holds buckets, the cluster was bootstrapped before.
	var lastErr error
	var bucketCount uint64

	for name, rs := range nameToReplicasetRef {
		// Ask the master, as vshard.router.bootstrap does: a lagging replica
		// may not have the buckets yet and make the cluster look empty.
		count, err := rs.bucketsCount(ctx, pool.ModeRW)
		if err != nil {
			err = fmt.Errorf("buckets_count on replicaset %s: %w", name, err)
			if !ifNotBootstrapped {
				return err
			}
			// As in vshard.router.bootstrap, one non-empty replicaset is enough
			// to consider the cluster bootstrapped, so keep asking the others.
			lastErr = err

			continue
		}

		if count > 0 && !ifNotBootstrapped {
			return newVShardErrorNonEmpty()
		}

		bucketCount += count
	}

	if bucketCount > 0 {
		if lastErr != nil {
			r.log().WarnContext(ctx, "cluster bootstrap skipped: buckets found, "+
				"but not every replicaset answered to check that none is missing",
				"error", lastErr)

			return nil
		}

		// buckets_count includes buckets being moved, so the sum may exceed
		// TotalBucketCount during rebalancing, but it never falls below it in a
		// bootstrapped cluster. A smaller sum means an earlier bootstrap stopped
		// halfway, and skipping it would leave the missing buckets unroutable.
		if bucketCount < r.cfg.TotalBucketCount {
			return fmt.Errorf("cluster is partially bootstrapped: found %d of %d buckets",
				bucketCount, r.cfg.TotalBucketCount)
		}

		r.log().InfoContext(ctx, "cluster bootstrap skipped: cluster is already bootstrapped",
			"bucket_count", bucketCount)

		return nil
	}

	if lastErr != nil {
		return lastErr
	}

	// nameToReplicasetRef is a map, so iterating it directly would make the bucket distribution non-deterministic
	names := make([]string, 0, len(nameToReplicasetRef))
	for name := range nameToReplicasetRef {
		names = append(names, name)
	}
	sort.Strings(names)

	rssToBootstrap := make([]Replicaset, 0, len(names))
	weightsUnset := true
	for _, name := range names {
		rs := *nameToReplicasetRef[name]
		weightsUnset = weightsUnset && rs.info.Weight == 0
		rssToBootstrap = append(rssToBootstrap, rs)
	}

	// Topology providers do not fill Weight, so it is usually zero everywhere,
	// and CalculateEtalonBalance would reject such a topology. vshard defaults
	// a replicaset weight to 1; do the same when no weight is set at all, and
	// keep the configured weights, zero ones included, otherwise. The slice
	// holds copies, so the router's replicasets are not changed.
	if weightsUnset {
		for i := range rssToBootstrap {
			rssToBootstrap[i].info.Weight = 1
		}
	}

	if err := CalculateEtalonBalance(rssToBootstrap, r.cfg.TotalBucketCount); err != nil {
		return err
	}

	bucketID := uint64(1)
	for _, rs := range rssToBootstrap {
		if rs.EtalonBucketCount > 0 {
			if err := rs.BucketForceCreate(ctx, bucketID, rs.EtalonBucketCount); err != nil {
				return err
			}

			nextBucketID := bucketID + rs.EtalonBucketCount
			r.log().InfoContext(ctx, "Buckets are bootstrapped", "replicaset", rs.String(), "from", bucketID, "to", nextBucketID-1)
			bucketID = nextBucketID
		}
	}

	r.log().InfoContext(ctx, "cluster bootstrap completed")

	return nil
}
