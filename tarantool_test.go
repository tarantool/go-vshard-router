package vshard_router_test //nolint:revive

import (
	"context"
	"fmt"
	"log"
	"math/rand"
	"os"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
	"github.com/tarantool/go-tarantool/v2"
	"github.com/tarantool/go-tarantool/v2/box"
	"github.com/tarantool/go-tarantool/v2/pool"
	"github.com/tarantool/go-tarantool/v2/test_helpers"
	vshardrouter "github.com/tarantool/go-vshard-router/v2"
	"github.com/tarantool/go-vshard-router/v2/providers/static"
	chelper "github.com/tarantool/go-vshard-router/v2/test_helper"
	"github.com/vmihailenco/msgpack/v5"
)

const (
	instancesCount   = 4
	totalBucketCount = 100
	username         = "guest"
)

// init servers from our cluster
var serverNames = map[string]string{
	// shard 1
	"storage_1_a": "127.0.0.1:3301",
	"storage_1_b": "127.0.0.1:3302",
	// shard 2
	"storage_2_a": "127.0.0.1:3303",
	"storage_2_b": "127.0.0.1:3304",
}

var topology = map[vshardrouter.ReplicasetInfo][]vshardrouter.InstanceInfo{
	{
		Name:   "storage_1",
		UUID:   uuid.New(),
		Weight: 1,
	}: {
		{
			Name: "storage_1_a",
			UUID: uuid.New(),
			Addr: "127.0.0.1:3301",
		},
		{
			Name: "storage_1_b",
			UUID: uuid.New(),
			Addr: "127.0.0.1:3302",
		},
	},
	{
		Name:   "storage_2",
		UUID:   uuid.New(),
		Weight: 1,
	}: {
		{
			Name: "storage_2_a",
			UUID: uuid.New(),
			Addr: "127.0.0.1:3303",
		},
		{
			Name: "storage_2_b",
			UUID: uuid.New(),
			Addr: "127.0.0.1:3304",
		},
	},
}

var noUUIDTopology = map[vshardrouter.ReplicasetInfo][]vshardrouter.InstanceInfo{
	{
		Name:   "storage_1",
		Weight: 1,
	}: {
		{
			Name: "storage_1_a",
			Addr: "127.0.0.1:3301",
		},
		{
			Name: "storage_1_b",
			Addr: "127.0.0.1:3302",
		},
	},
	{
		Name:   "storage_2",
		Weight: 1,
	}: {
		{
			Name: "storage_2_a",
			Addr: "127.0.0.1:3303",
		},
		{
			Name: "storage_2_b",
			Addr: "127.0.0.1:3304",
		},
	},
}

// for tarantool 3.0 uuid is not required
func TestNewRouter_IgnoreUUID(t *testing.T) {
	ctx := context.Background()

	_, err := vshardrouter.NewRouter(ctx, vshardrouter.Config{
		TotalBucketCount: 100,
		TopologyProvider: static.NewProvider(noUUIDTopology),
		User:             username,
	})

	require.NoError(t, err)
}

func TestRouter_Topology(t *testing.T) {
	t.Parallel()

	ctx := context.Background()

	router, err := vshardrouter.NewRouter(ctx, vshardrouter.Config{
		TopologyProvider: static.NewProvider(topology),
		DiscoveryTimeout: 5 * time.Second,
		DiscoveryMode:    vshardrouter.DiscoveryModeOn,
		TotalBucketCount: totalBucketCount,
		User:             username,
	})

	require.Nil(t, err, "NewRouter finished successfully")

	var rsInfo vshardrouter.ReplicasetInfo
	var insInfo vshardrouter.InstanceInfo
	for k, replicas := range topology {
		if len(replicas) == 0 {
			continue
		}
		rsInfo = k
		//nolint:gosec
		insInfo = replicas[rand.Int()%len(replicas)]
	}

	tCtrl := router.Topology()

	// remove some random replicaset
	_ = tCtrl.RemoveReplicaset(ctx, rsInfo.Name)
	// add it again
	err = tCtrl.AddReplicasets(ctx, map[vshardrouter.ReplicasetInfo][]vshardrouter.InstanceInfo{rsInfo: topology[rsInfo]})
	require.Nil(t, err, "AddReplicasets finished successfully")

	// remove some random instance
	err = tCtrl.RemoveInstance(ctx, rsInfo.Name, insInfo.Name)
	require.Nil(t, err, "RemoveInstance finished successfully")

	// add it again
	err = tCtrl.AddInstance(ctx, rsInfo.Name, insInfo)
	require.Nil(t, err, "AddInstance finished successfully")
}

type CustomDecodingStruct struct {
	Name string
	Age  int
}

func (c *CustomDecodingStruct) DecodeMsgpack(d *msgpack.Decoder) error {
	arrLen, err := d.DecodeArrayLen()
	if err != nil {
		return err
	}

	if arrLen != 2 {
		return fmt.Errorf("length must be equal 2")
	}

	name, err := d.DecodeString()
	if err != nil {
		return err
	}

	c.Name = name

	age, err := d.DecodeInt()
	if err != nil {
		return err
	}

	c.Age = age

	return nil
}

func TestRouter_Call(t *testing.T) {
	t.Parallel()

	ctx := context.Background()

	router, err := vshardrouter.NewRouter(ctx, vshardrouter.Config{
		TopologyProvider: static.NewProvider(topology),
		DiscoveryTimeout: 5 * time.Second,
		DiscoveryMode:    vshardrouter.DiscoveryModeOn,
		TotalBucketCount: totalBucketCount,
		User:             username,
	})
	require.NoError(t, err, "NewRouter started successfully")

	bucketID := randBucketID(totalBucketCount)

	rs, err := router.Route(ctx, bucketID)
	require.NoError(t, err, "router.Route with no err")

	t.Run("proto test", func(t *testing.T) {
		const maxRespLen = 3
		for argLen := 0; argLen <= maxRespLen; argLen++ {
			args := make([]interface{}, 0, argLen)

			for i := 0; i < argLen; i++ {
				args = append(args, "arg")
			}

			var routerOpts vshardrouter.CallOpts
			resp, err := router.CallRW(ctx, bucketID, "echo", args, routerOpts)
			require.NoError(t, err, "router.CallRW with no err")

			var resViaVshard interface{}
			var resDirect interface{}
			var resGet []interface{}

			err = resp.GetTyped(&resViaVshard)
			require.NoError(t, err, "GetTyped with no err")

			resGet, err = resp.Get()
			require.NoError(t, err, "Get with no err")

			require.Equal(t, resViaVshard, resGet, "resViaVshard and resGet are equal")

			var rsOpts vshardrouter.ReplicasetCallOpts

			err = rs.CallAsync(ctx, rsOpts, "echo", args).GetTyped(&resDirect)
			require.NoError(t, err, "rs.CallAsync.GetTyped with no error")

			require.Equalf(t, resDirect, resViaVshard, "resDirect != resViaVshard on argLen %d", argLen)
		}
	})

	t.Run("custom decoders works valid", func(t *testing.T) {
		res := &CustomDecodingStruct{}
		args := []interface{}{"Maksim", 21}

		resp, err := router.CallRW(ctx, bucketID, "echo", &args, vshardrouter.CallOpts{})
		require.NoError(t, err, "router.CallRW with no err")

		err = resp.GetTyped(res)
		require.NoError(t, err)

		require.Equal(t, &CustomDecodingStruct{Name: "Maksim", Age: 21}, res)
	})

	t.Run("router.Call err", func(t *testing.T) {
		callMode := vshardrouter.CallModeRO
		args := []interface{}{}
		callOpts := vshardrouter.CallOpts{}

		_, err := router.Call(ctx, totalBucketCount+1, callMode, "echo", args, callOpts)
		require.Error(t, err, "RouterCall echo finished with err when bucketID is out of range")

		_, err = router.Call(ctx, 0, callMode, "echo", args, callOpts)
		require.Error(t, err, "RouterCall echo finished with err when bucketID is 0")

		_, err = router.Call(ctx, bucketID, callMode, "echo", nil, callOpts)
		require.Error(t, err, "RouterCall echo finised with err on nil args")

		_, err = router.Call(ctx, bucketID, callMode, "raise_luajit_error", args, callOpts)
		require.NotNil(t, err, "RouterCall raise_luajit_error finished with err")

		_, err = router.Call(ctx, bucketID, callMode, "raise_client_error", args, callOpts)
		require.NotNil(t, err, "RouterCall raise_client_error finished with err")
	})

	t.Run("router.Call simulate vshard error", func(t *testing.T) {
		rsMap := router.RouteAll()

		// 1. Replace replicaset for bucketID
		for k, v := range rsMap {
			if rs != v {
				res, err := router.BucketSet(bucketID, k)
				require.Nil(t, err, "BucketSet finished with no err")
				require.Equal(t, res, v)
				break
			}
		}

		// 2. Try to call something
		_, err = router.Call(ctx, bucketID, vshardrouter.CallModeRO, "echo", []interface{}{}, vshardrouter.CallOpts{})
		require.Nil(t, err, "RouterCallImpl echo finished with no err even on dirty bucket map")
	})
}

func randBucketID(totalBucketCount uint64) uint64 {
	//nolint:gosec
	return (rand.Uint64() % totalBucketCount) + 1
}

// BENCH

type Product struct {
	BucketID uint64 `msgpack:"bucket_id"`
	ID       string `msgpack:"id"`
	Name     string `msgpack:"name"`
	Count    uint64 `msgpack:"count"`
}

func BenchmarkCallSimpleInsert_GO_Call(b *testing.B) {
	b.StopTimer()

	ctx := context.Background()

	router, err := vshardrouter.NewRouter(ctx, vshardrouter.Config{
		TopologyProvider: static.NewProvider(topology),
		DiscoveryTimeout: 5 * time.Second,
		DiscoveryMode:    vshardrouter.DiscoveryModeOn,
		TotalBucketCount: totalBucketCount,
		User:             username,
		RequestTimeout:   time.Minute,
	})
	require.NoError(b, err)

	b.StartTimer()
	for i := 0; i < b.N; i++ {
		id := uuid.New()

		bucketID := router.BucketIDStrCRC32(id.String())
		_, err := router.Call(
			ctx,
			bucketID,
			vshardrouter.CallModeRW,
			"product_add",
			[]interface{}{&Product{Name: "test-go", BucketID: bucketID, ID: id.String(), Count: 3}},
			vshardrouter.CallOpts{Timeout: 10 * time.Second})
		require.NoError(b, err)
	}

	b.ReportAllocs()
}

func BenchmarkCallSimpleSelect_GO_Call(b *testing.B) {
	b.StopTimer()

	ctx := context.Background()

	router, err := vshardrouter.NewRouter(ctx, vshardrouter.Config{
		TopologyProvider: static.NewProvider(topology),
		DiscoveryTimeout: 5 * time.Second,
		DiscoveryMode:    vshardrouter.DiscoveryModeOn,
		TotalBucketCount: totalBucketCount,
		User:             username,
	})
	require.NoError(b, err)

	ids := make([]uuid.UUID, b.N)

	for i := 0; i < b.N; i++ {
		id := uuid.New()
		ids[i] = id

		bucketID := router.BucketIDStrCRC32(id.String())
		_, err := router.Call(
			ctx,
			bucketID,
			vshardrouter.CallModeRW,
			"product_add",
			[]interface{}{&Product{Name: "test-go", BucketID: bucketID, ID: id.String(), Count: 3}},
			vshardrouter.CallOpts{},
		)
		require.NoError(b, err)
	}

	type Request struct {
		ID string `msgpack:"id"`
	}

	b.StartTimer()
	for i := 0; i < b.N; i++ {
		id := ids[i]

		bucketID := router.BucketIDStrCRC32(id.String())
		resp, err1 := router.Call(
			ctx,
			bucketID,
			vshardrouter.CallModeBRO,
			"product_get",
			[]interface{}{&Request{ID: id.String()}},
			vshardrouter.CallOpts{Timeout: time.Second},
		)

		var product Product

		err2 := resp.GetTyped(&[]interface{}{&product})

		b.StopTimer()
		require.NoError(b, err1)
		require.NoError(b, err2)
		b.StartTimer()
	}

	b.ReportAllocs()
}

func BenchmarkCallSimpleInsert_Lua(b *testing.B) {
	b.StopTimer()

	ctx := context.Background()
	dialer := tarantool.NetDialer{
		Address: "0.0.0.0:12000",
	}

	instances := []pool.Instance{{
		Name:   "router",
		Dialer: dialer,
	}}

	p, err := pool.Connect(ctx, instances)
	require.NoError(b, err)
	require.NotNil(b, p)

	b.StartTimer()
	for i := 0; i < b.N; i++ {
		id := uuid.New()
		req := tarantool.NewCallRequest("api.add_product").
			Context(ctx).
			Args([]interface{}{&Product{Name: "test-lua", ID: id.String(), Count: 3}})

		feature := p.Do(req, pool.ANY)
		faces, err := feature.Get()

		require.NoError(b, err)
		require.NotNil(b, faces)
	}

	b.ReportAllocs()
}

func BenchmarkCallSimpleSelect_Lua(b *testing.B) {
	b.StopTimer()

	ctx := context.Background()

	router, err := vshardrouter.NewRouter(ctx, vshardrouter.Config{
		TopologyProvider: static.NewProvider(topology),
		DiscoveryTimeout: 5 * time.Second,
		DiscoveryMode:    vshardrouter.DiscoveryModeOn,
		TotalBucketCount: totalBucketCount,
		User:             username,
	})
	require.NoError(b, err)

	ids := make([]uuid.UUID, b.N)

	for i := 0; i < b.N; i++ {
		id := uuid.New()
		ids[i] = id

		bucketID := router.BucketIDStrCRC32(id.String())
		_, err := router.Call(
			ctx,
			bucketID,
			vshardrouter.CallModeRW,
			"product_add",
			[]interface{}{&Product{Name: "test-go", BucketID: bucketID, ID: id.String(), Count: 3}},
			vshardrouter.CallOpts{})
		require.NoError(b, err)
	}

	type Request struct {
		ID string `msgpack:"id"`
	}

	dialer := tarantool.NetDialer{
		Address: "0.0.0.0:12000",
		User:    username,
	}

	instances := []pool.Instance{{
		Name:   "router",
		Dialer: dialer,
	}}

	p, err := pool.Connect(ctx, instances)
	require.NoError(b, err)
	require.NotNil(b, p)

	b.StartTimer()
	for i := 0; i < b.N; i++ {
		id := ids[i]

		req := tarantool.NewCallRequest("api.get_product").
			Context(ctx).
			Args([]interface{}{&Request{ID: id.String()}})

		feature := p.Do(req, pool.ANY)
		var product Product
		err = feature.GetTyped(&[]interface{}{&product})

		b.StopTimer()
		require.NoError(b, err)
		b.StartTimer()
	}

	b.ReportAllocs()
}

func TestRouter_RouterMapCallRW(t *testing.T) {
	t.Parallel()

	ctx := context.Background()

	router, err := vshardrouter.NewRouter(ctx, vshardrouter.Config{
		TopologyProvider: static.NewProvider(topology),
		DiscoveryTimeout: 5 * time.Second,
		DiscoveryMode:    vshardrouter.DiscoveryModeOn,
		TotalBucketCount: totalBucketCount,
		User:             username,
	})
	require.Nil(t, err, "NewRouter created successfully")

	callOpts := vshardrouter.RouterMapCallRWOptions{}

	const arg = "arg1"

	// Enusre that RouterMapCallRWImpl works at all
	echoArgs := []interface{}{arg}
	respStr, err := vshardrouter.RouterMapCallRW[string](router, ctx, "echo", echoArgs, callOpts)
	require.NoError(t, err, "RouterMapCallRWImpl echo finished with no err")

	for k, v := range respStr {
		require.Equalf(t, arg, v, "RouterMapCallRWImpl value ok for %v", k)
	}

	echoArgs = []interface{}{1}
	respInt, err := vshardrouter.RouterMapCallRW[int](router, ctx, "echo", echoArgs, vshardrouter.RouterMapCallRWOptions{})
	require.NoError(t, err, "RouterMapCallRW[int] echo finished with no err")
	for k, v := range respInt {
		require.Equalf(t, 1, v, "RouterMapCallRW[int] value ok for %v", k)
	}

	// RouterMapCallRWImpl returns only one value
	echoArgs = []interface{}{arg, "arg2"}
	respStr, err = vshardrouter.RouterMapCallRW[string](router, ctx, "echo", echoArgs, callOpts)
	require.NoError(t, err, "RouterMapCallRWImpl echo finished with no err")

	for k, v := range respStr {
		require.Equalf(t, arg, v, "RouterMapCallRWImpl value ok for %v", k)
	}

	// RouterMapCallRWImpl returns nil when no return value
	noArgs := []interface{}{}
	resp, err := vshardrouter.RouterMapCallRW[interface{}](router, ctx, "echo", noArgs, callOpts)
	require.NoError(t, err, "RouterMapCallRWImpl echo finished with no err")

	for k, v := range resp {
		require.Equalf(t, nil, v, "RouterMapCallRWImpl value ok for %v", k)
	}

	// Ensure that RouterMapCallRWImpl sends requests concurrently
	const sleepToSec int = 1
	sleepArgs := []interface{}{sleepToSec}

	start := time.Now()
	_, err = vshardrouter.RouterMapCallRW[interface{}](router, ctx, "sleep", sleepArgs, vshardrouter.RouterMapCallRWOptions{
		Timeout: 2 * time.Second, // because default timeout is 0.5 sec
	})
	duration := time.Since(start)

	require.NoError(t, err, "RouterMapCallRWImpl sleep finished with no err")
	require.Greater(t, len(topology), 1, "There are more than one replicasets")
	require.Less(t, duration, 1200*time.Millisecond, "Requests were send concurrently")

	// RouterMapCallRWImpl returns err on raise_luajit_error
	_, err = vshardrouter.RouterMapCallRW[interface{}](router, ctx, "raise_luajit_error", noArgs, callOpts)
	require.NotNil(t, err, "RouterMapCallRWImpl raise_luajit_error finished with error")

	// RouterMapCallRWImpl invalid usage
	_, err = vshardrouter.RouterMapCallRW[interface{}](router, ctx, "echo", nil, callOpts)
	require.NotNil(t, err, "RouterMapCallRWImpl with nil args finished with error")

	// Ensure that RouterMapCallRWImpl doesn't work when it mean't to
	for rsInfo := range topology {
		errs := router.RemoveReplicaset(ctx, rsInfo.Name)
		require.Emptyf(t, errs, "%s successfully removed from router", rsInfo.Name)
		break
	}

	_, err = vshardrouter.RouterMapCallRW[interface{}](router, ctx, "echo", echoArgs, callOpts)
	require.NotNilf(t, err, "RouterMapCallRWImpl failed on not full cluster")
}

func TestRouterRoute(t *testing.T) {
	t.Parallel()

	var modes = []vshardrouter.BucketsSearchMode{
		vshardrouter.BucketsSearchLegacy,
		vshardrouter.BucketsSearchBatchedQuick,
		vshardrouter.BucketsSearchBatchedFull,
	}

	for _, mode := range modes {
		testRouterRouteWithMode(t, mode)
	}
}

func testRouterRouteWithMode(t *testing.T, searchMode vshardrouter.BucketsSearchMode) {
	ctx := context.Background()

	router, err := vshardrouter.NewRouter(ctx, vshardrouter.Config{
		TopologyProvider:  static.NewProvider(topology),
		DiscoveryTimeout:  5 * time.Second,
		DiscoveryMode:     vshardrouter.DiscoveryModeOn,
		BucketsSearchMode: searchMode,
		TotalBucketCount:  totalBucketCount,
		User:              username,
	})

	require.Nilf(t, err, "NewRouter finished successfully, mode %v", searchMode)

	_, err = router.Route(ctx, totalBucketCount+1)
	require.Error(t, err, "invalid bucketID")

	// pick some random bucket
	bucketID := randBucketID(totalBucketCount)

	// clean everything
	router.RouteMapClean()

	// resolve it
	rs, err := router.Route(ctx, bucketID)
	require.Nilf(t, err, "router.Route ok, mode %v", searchMode)

	// reset it again
	router.BucketReset(bucketID)

	// call RouteAll, because:
	// 1. increase coverage
	// 2. we cannot get replicaset uuid by rs instance (lack of interface)
	rsMap := router.RouteAll()
	for k, v := range rsMap {
		if v == rs {
			// set it again
			res, err := router.BucketSet(bucketID, k)
			require.Nil(t, err, nil, "BucketSet ok")
			require.Equal(t, res, rs, "BucketSet res ok")
			break
		}
	}
}

// TestReplicaset_Pooler tests that pooler logic works ok with replicaset.
func TestReplicaset_Pooler(t *testing.T) {
	ctx := context.Background()

	router, err := vshardrouter.NewRouter(ctx, vshardrouter.Config{
		TopologyProvider: static.NewProvider(topology),
		DiscoveryTimeout: 5 * time.Second,
		DiscoveryMode:    vshardrouter.DiscoveryModeOn,
		TotalBucketCount: totalBucketCount,
		User:             username,
	})
	require.Nil(t, err, "NewRouter created successfully")

	t.Run("go-tarantool box module works ok with go-vshard replicaset", func(t *testing.T) {
		// check that masters are alive
		for _, rs := range router.RouteAll() {
			b := box.New(pool.NewConnectorAdapter(rs.Pooler(), pool.RW))
			require.NotNil(t, b)

			info, err := b.Info()
			require.NoError(t, err, "master respond info")
			require.False(t, info.RO, "it is not RO")
		}
	})
}

func TestDegradedCluster(t *testing.T) {
	ctx := context.Background()

	// create a topology to imitate cluster with several replicasets:
	// 1 fake replicaset (imitates an unavailable replicaset) + all replicasets of topology
	topologyDegraded := map[vshardrouter.ReplicasetInfo][]vshardrouter.InstanceInfo{
		{
			// add fake replicaset
			Name:   "storage_0",
			UUID:   uuid.New(),
			Weight: 1,
		}: {
			{
				Name: "storage_0_a",
				UUID: uuid.New(),
				Addr: "127.0.0.1:2998",
			},
			{
				Name: "storage_0_b",
				UUID: uuid.New(),
				Addr: "127.0.0.1:2999",
			},
		},
	}

	// add all replicasets of topology
	for k, v := range topology {
		topologyDegraded[k] = v
	}

	router, err := vshardrouter.NewRouter(ctx, vshardrouter.Config{
		TopologyProvider: static.NewProvider(topologyDegraded),
		DiscoveryTimeout: 5 * time.Second,
		DiscoveryMode:    vshardrouter.DiscoveryModeOn,
		TotalBucketCount: totalBucketCount,
		User:             username,
	})
	require.NoError(t, err, "NewRouter created successfully")

	for bucketID := uint64(1); bucketID <= totalBucketCount; bucketID++ {
		_, err := router.Route(ctx, bucketID)
		require.NoErrorf(t, err, "bucket %d resolved successfully")
	}
}

func TestReplicsetCallAsync(t *testing.T) {
	t.Parallel()

	ctx := context.Background()

	router, err := vshardrouter.NewRouter(ctx, vshardrouter.Config{
		TopologyProvider: static.NewProvider(topology),
		DiscoveryTimeout: 5 * time.Second,
		DiscoveryMode:    vshardrouter.DiscoveryModeOn,
		TotalBucketCount: totalBucketCount,
		User:             username,
	})

	require.Nil(t, err, "NewRouter finished successfully")

	rsMap := router.RouteAll()

	var rs *vshardrouter.Replicaset
	// pick random rs
	for _, v := range rsMap {
		rs = v
		break
	}

	callOpts := vshardrouter.ReplicasetCallOpts{
		PoolMode: pool.ANY,
	}

	// Tests for arglen ans response parsing
	future := rs.CallAsync(ctx, callOpts, "echo", nil)
	resp, err := future.Get()
	require.Nil(t, err, "CallAsync finished with no err on nil args")
	require.Equal(t, resp, []interface{}{}, "CallAsync returns empty arr on nil args")
	var typed interface{}
	err = future.GetTyped(&typed)
	require.Nil(t, err, "GetTyped finished with no err on nil args")
	require.Equal(t, []interface{}{}, resp, "GetTyped returns empty arr on nil args")

	const checkUpTo = 100
	for argLen := 1; argLen <= checkUpTo; argLen++ {
		args := []interface{}{}

		for i := 0; i < argLen; i++ {
			args = append(args, "arg")
		}

		future := rs.CallAsync(ctx, callOpts, "echo", args)
		resp, err := future.Get()
		require.Nilf(t, err, "CallAsync finished with no err for argLen %d", argLen)
		require.Equalf(t, args, resp, "CallAsync resp ok for argLen %d", argLen)

		var typed interface{}
		err = future.GetTyped(&typed)
		require.Nilf(t, err, "GetTyped finished with no err for argLen %d", argLen)
		require.Equal(t, args, typed, "GetTyped resp ok for argLen %d", argLen)
	}

	// Test for async execution
	timeBefore := time.Now()

	var futures = make([]*tarantool.Future, 0, len(rsMap))
	for _, rs := range rsMap {
		future := rs.CallAsync(ctx, callOpts, "sleep", []interface{}{1})
		futures = append(futures, future)
	}

	for i, future := range futures {
		_, err := future.Get()
		require.Nil(t, err, "future[%d].Get finished with no err for async test", i)
	}

	duration := time.Since(timeBefore)
	require.True(t, len(rsMap) > 1, "Async test: more than one replicaset")
	require.Less(t, duration, 1200*time.Millisecond, "Async test: requests were sent concurrently")

	// Test no timeout by default
	future = rs.CallAsync(ctx, callOpts, "sleep", []interface{}{1})
	_, err = future.Get()
	require.Nil(t, err, "CallAsync no timeout by default")

	// Test for timeout via ctx
	ctxTimeout, cancel := context.WithTimeout(ctx, 500*time.Millisecond)
	defer cancel()
	future = rs.CallAsync(ctxTimeout, callOpts, "sleep", []interface{}{1})
	_, err = future.Get()
	require.NotNil(t, err, "CallAsync timeout by context does work")

	// Test for timeout via config
	callOptsTimeout := vshardrouter.ReplicasetCallOpts{
		PoolMode: pool.ANY,
		Timeout:  500 * time.Millisecond,
	}
	future = rs.CallAsync(ctx, callOptsTimeout, "sleep", []interface{}{1})
	_, err = future.Get()
	require.NotNil(t, err, "CallAsync timeout by callOpts does work")

	future = rs.CallAsync(ctx, callOpts, "raise_luajit_error", nil)
	_, err = future.Get()
	require.NotNil(t, err, "raise_luajit_error returns error")

	future = rs.CallAsync(ctx, callOpts, "raise_client_error", nil)
	_, err = future.Get()
	require.NotNil(t, err, "raise_client_error returns error")
}

func runTestMain(m *testing.M) int {
	dialers := make([]tarantool.NetDialer, instancesCount)
	opts := make([]test_helpers.StartOpts, instancesCount)

	i := 0
	for name, addr := range serverNames {
		dialers[i] = tarantool.NetDialer{
			Address: addr,
			User:    username,
		}

		opts[i] = test_helpers.StartOpts{
			Dialer:       dialers[i],
			InitScript:   "config.lua",
			Listen:       addr,
			WaitStart:    100 * time.Millisecond,
			ConnectRetry: 100,
			RetryTimeout: 500 * time.Millisecond,
			WorkDir:      name, // this is not wrong
		}

		i++
	}

	instances, err := chelper.StartTarantoolInstances(opts)
	if err != nil {
		log.Printf("Failed to prepare test Tarantool: %s", err)
		return 1
	}

	inst, err := test_helpers.StartTarantool(test_helpers.StartOpts{
		Dialer:       tarantool.NetDialer{Address: "0.0.0.0:12000", User: username},
		InitScript:   "config.lua",
		Listen:       "0.0.0.0:12000",
		WaitStart:    100 * time.Millisecond,
		ConnectRetry: 100,
		RetryTimeout: 500 * time.Millisecond,
		WorkDir:      "router",
	})

	if err != nil {
		log.Printf("Failed to prepare test Tarantool: %s", err)
		return 1
	}

	defer test_helpers.StopTarantoolWithCleanup(inst)
	defer test_helpers.StopTarantoolInstances(instances)

	return m.Run()
}

func TestMain(m *testing.M) {
	code := runTestMain(m)
	os.Exit(code)
}
