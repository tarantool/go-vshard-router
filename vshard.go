package vshard_router //nolint:revive

import (
	"context"
	"fmt"
	"sync/atomic"
	"time"

	"github.com/google/uuid"
	"github.com/snksoft/crc"
	"github.com/vmihailenco/msgpack/v5"

	tarantool "github.com/tarantool/go-tarantool/v2"
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

type routeMap = []atomic.Pointer[Replicaset]

type Router struct {
	cfg Config

	// nameToReplicaset is an atomic pointer, that points to an immutable object by our convention.
	// Whenever we add or remove a replicaset, we create a new map object.
	// Object under nameToReplicaset can be replaced to a new one only by TopologyController methods.
	// Assuming that we rarely add or remove some replicaset,
	// it should be the simplest and most efficient way of handling concurrent access.
	// Additionally, we can safely iterate over a map because it never changes.
	nameToReplicaset atomic.Pointer[map[string]*Replicaset]

	routeMap atomic.Pointer[routeMap]

	// refID is used for Map-Reduce operation. Since it must be unique (within connection) for each ref request,
	// we made it global and monotonically growing for each Router instance.
	refID atomic.Int64

	cancelDiscovery func()
}

func (r *Router) metrics() MetricsProvider {
	return r.cfg.Metrics
}

func (r *Router) log() LogfProvider {
	return r.cfg.Loggerf
}

func (r *Router) getRouteMap() routeMap {
	ptr := r.routeMap.Load()
	return *ptr
}

func (r *Router) setRouteMap(routeMap routeMap) {
	r.routeMap.Store(&routeMap)
}

type Config struct {
	// Providers
	// Loggerf injects a custom logger. By default there is no logger is used.
	Loggerf          LogfProvider     // Loggerf is not required
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

	TotalBucketCount uint64
	User             string
	Password         string
	PoolOpts         tarantool.Opts

	// BucketGetter is an optional argument.
	// You can specify a function that will receive the bucket id from the context.
	// This is useful if you use middleware that inserts the calculated bucket id into the request context.
	BucketGetter func(ctx context.Context) uint64
	// RequestTimeout timeout for requests to Tarantool.
	// Don't rely on using this timeout.
	// This is the difference between the timeout of the library itself
	// that is, our retry timeout if the buckets, for example, move.
	// Currently, it only works for sugar implementations .
	RequestTimeout time.Duration
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

	cfg, err = prepareCfg(ctx, cfg)
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
		router.log().Errorf(ctx, "Can't create new topology provider with err: %s", err)

		return nil, fmt.Errorf("%w; cant init topology with err: %w", ErrTopologyProvider, err)
	}

	err = router.DiscoveryAllBuckets(ctx)
	if err != nil {
		router.log().Errorf(ctx, "router.DiscoveryAllBuckets failed: %v", err)
	}

	if cfg.DiscoveryMode == DiscoveryModeOn {
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
	if bucketID < 1 || r.cfg.TotalBucketCount < bucketID {
		return nil, fmt.Errorf("bucket id is out of range: %d (total %d)", bucketID, r.cfg.TotalBucketCount)
	}

	nameToReplicasetRef := r.getNameToReplicaset()
	routeMap := r.getRouteMap()

	return r.bucketSet(nameToReplicasetRef, routeMap, bucketID, rsName)
}

func (r *Router) bucketSet(nameToReplicasetRef map[string]*Replicaset, routeMap routeMap, bucketID uint64, rsName string) (*Replicaset, error) {
	rs := nameToReplicasetRef[rsName]
	if rs == nil {
		return nil, newVShardErrorNoRouteToBucket(bucketID)
	}

	routeMap[bucketID].Store(rs)

	return rs, nil
}

func (r *Router) BucketReset(bucketID uint64) {
	if bucketID < 1 || r.cfg.TotalBucketCount < bucketID {
		return
	}

	routeMap := r.getRouteMap()
	r.bucketReset(routeMap, bucketID)
}

func (r *Router) bucketReset(routeMap routeMap, bucketID uint64) {
	routeMap[bucketID].Store(nil)
}

func (r *Router) RouteMapClean() {
	r.setEmptyRouteMap()
}

func (r *Router) setEmptyRouteMap() {
	routeMap := make([]atomic.Pointer[Replicaset], r.cfg.TotalBucketCount+1)
	r.setRouteMap(routeMap)
}

func prepareCfg(ctx context.Context, cfg Config) (Config, error) {
	const discoveryTimeoutDefault = 1 * time.Minute
	const discoveryWorkStepDefault = 10 * time.Millisecond

	err := validateCfg(cfg)
	if err != nil {
		return Config{}, fmt.Errorf("%v: %v", ErrInvalidConfig, err)
	}

	if cfg.DiscoveryTimeout == 0 {
		cfg.DiscoveryTimeout = discoveryTimeoutDefault
	}

	if cfg.Loggerf == nil {
		cfg.Loggerf = emptyLogfProvider
	}

	// Log tarantool internal events using the same logger as router uses.
	cfg.PoolOpts.Logger = tarantoolOptsLogger{
		loggerf: cfg.Loggerf,
		ctx:     ctx,
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

// ClusterBootstrap initializes the cluster by bootstrapping the necessary buckets
// across the available replicasets. It checks the current state of each replicaset
// and creates buckets if required. The function takes a context for managing
// cancellation and deadlines, and a boolean parameter ifNotBootstrapped to control
// error handling. If ifNotBootstrapped is true, the function will log any errors
// encountered during the bootstrapping process but will not halt execution; instead,
// it will return the last error encountered. If ifNotBootstrapped is false, any
// error will result in an immediate return, ensuring that the operation either
// succeeds fully or fails fast.
// Deprecated: use lua bootstrap now, go-router bootstrap now works invalid.
func (r *Router) ClusterBootstrap(ctx context.Context, ifNotBootstrapped bool) error {
	nameToReplicasetRef := r.getNameToReplicaset()

	rssToBootstrap := make([]Replicaset, 0, len(nameToReplicasetRef))
	var lastErr error

	for _, rs := range nameToReplicasetRef {
		rssToBootstrap = append(rssToBootstrap, *rs)
	}

	err := CalculateEtalonBalance(rssToBootstrap, r.cfg.TotalBucketCount)
	if err != nil {
		return err
	}

	bucketID := uint64(1)
	for id, rs := range rssToBootstrap {
		if rs.EtalonBucketCount > 0 {
			err = rs.BucketForceCreate(ctx, bucketID, rs.EtalonBucketCount)
			if err != nil {
				if ifNotBootstrapped {
					lastErr = err
				} else {
					return err
				}
			} else {
				nextBucketID := bucketID + rs.EtalonBucketCount
				r.log().Infof(ctx, "Buckets from %d to %d are bootstrapped on \"%s\"", bucketID, nextBucketID-1, id)
				bucketID = nextBucketID
			}
		}
	}

	if lastErr != nil {
		return lastErr
	}

	return nil
}
