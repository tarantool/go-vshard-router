package tnt

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/tarantool/go-tarantool/v2/pool"
	vshardrouter "github.com/tarantool/go-vshard-router"
	"github.com/tarantool/go-vshard-router/providers/static"
)

func TestRouterCallImplProto(t *testing.T) {
	skipOnInvalidRun(t)

	t.Parallel()

	ctx := context.Background()

	cfg := getCfg()

	router, err := vshardrouter.NewRouter(ctx, vshardrouter.Config{
		TopologyProvider: static.NewProvider(cfg),
		DiscoveryTimeout: 5 * time.Second,
		DiscoveryMode:    vshardrouter.DiscoveryModeOn,
		TotalBucketCount: totalBucketCount,
		User:             defaultTntUser,
		Password:         defaultTntPassword,
	})

	require.Nil(t, err, "NewRouter finished successfully")

	bucketID := randBucketID(totalBucketCount)
	arg1, arg2 := "arg1", "arg2"
	args := []interface{}{arg1, arg2}
	callOpts := vshardrouter.CallOpts{
		VshardMode: vshardrouter.ReadMode,
		PoolMode:   pool.PreferRO,
	}

	resp, getTyped, err := router.RouterCallImpl(ctx, bucketID, callOpts, "echo", args)
	require.Nil(t, err, "RouterCallImpl echo finished with no err")
	require.EqualValues(t, args, resp, "RouterCallImpl echo resp correct")
	var arg1Got, arg2Got string
	err = getTyped(&arg1Got, &arg2Got)
	require.Nil(t, err, "RouterCallImpl getTyped call ok")
	require.Equal(t, arg1, arg1Got, "RouterCallImpl getTyped arg1 res ok")
	require.Equal(t, arg2, arg2Got, "RouterCallImpl getTyped arg2 res ok")

	_, _, err = router.RouterCallImpl(ctx, totalBucketCount+1, callOpts, "echo", args)
	require.Error(t, err, "RouterCallImpl echo finished with err when bucketID is out of range")

	_, _, err = router.RouterCallImpl(ctx, 0, callOpts, "echo", args)
	require.Error(t, err, "RouterCallImpl echo finished with err when bucketID is 0")

	_, _, err = router.RouterCallImpl(ctx, bucketID, callOpts, "echo", nil)
	require.NotNil(t, err, "RouterCallImpl echo finised with nil args")

	_, _, err = router.RouterCallImpl(ctx, bucketID, callOpts, "raise_luajit_error", args)
	require.NotNil(t, err, "RouterCallImpl raise_luajit_error finished with err")

	_, _, err = router.RouterCallImpl(ctx, bucketID, callOpts, "raise_client_error", args)
	require.NotNil(t, err, "RouterCallImpl raise_client_error finished with err")

	// maxRespLen is due to:
	// https://github.com/tarantool/vshard/blob/dfa2cc8a2aff221d5f421298851a9a229b2e0434/vshard/storage/init.lua#L3130
	const maxRespLen = 3
	for argLen := 0; argLen <= maxRespLen+1; argLen++ {
		args := []interface{}{}
		for i := 0; i < argLen; i++ {
			args = append(args, "arg")
		}

		resp, getTyped, err = router.RouterCallImpl(ctx, bucketID, callOpts, "echo", args)
		require.Nilf(t, err, "RouterCallImpl no err for arglen %d", argLen)

		expect := args
		if argLen > maxRespLen {
			expect = expect[:maxRespLen]
		}

		require.Equal(t, expect, resp, "RouterCallImpl resp ok for arglen %d", argLen)
		var typed interface{}
		err = getTyped(&typed)
		require.Nil(t, err, "RouterCallImpl getTyped no err for arglen %d", argLen)

		if argLen > 0 {
			// TODO: Should we handle multiple return values in getTyped?
			require.Equal(t, expect[0], typed, "RouterCallImpl getTyped resp ok for arglen %d", argLen)
		}
	}

	// simulate vshard error

	// 1. Replace replicaset for bucketID
	rs, err := router.BucketResolve(ctx, bucketID)
	require.Nil(t, err, "BucketResolve finished with no err")
	rsMap := router.RouterRouteAll()

	for k, v := range rsMap {
		if rs != v {
			res, err := router.BucketSet(bucketID, k)
			require.Nil(t, err, "BucketSet finished with no err")
			require.Equal(t, res, v)
			break
		}
	}

	// 2. Try to call something
	_, _, err = router.RouterCallImpl(ctx, bucketID, callOpts, "echo", args)
	require.Nil(t, err, "RouterCallImpl echo finished with no err even on dirty bucket map")
}

func TestRouterCallProto(t *testing.T) {
	skipOnInvalidRun(t)

	t.Parallel()

	ctx := context.Background()

	cfg := getCfg()

	router, err := vshardrouter.NewRouter(ctx, vshardrouter.Config{
		TopologyProvider: static.NewProvider(cfg),
		DiscoveryTimeout: 5 * time.Second,
		DiscoveryMode:    vshardrouter.DiscoveryModeOn,
		TotalBucketCount: totalBucketCount,
		User:             defaultTntUser,
		Password:         defaultTntPassword,
	})
	require.NoError(t, err, "NewRouter finished successfully")

	bucketID := randBucketID(totalBucketCount)

	rs, err := router.BucketResolve(ctx, bucketID)
	require.NoError(t, err, "BucketResolve with no err")

	const maxRespLen = 3
	for argLen := 0; argLen <= maxRespLen; argLen++ {
		args := []interface{}{}

		for i := 0; i < argLen; i++ {
			args = append(args, "arg")
		}

		var routerOpts vshardrouter.VshardRouterCallOptions
		resp, err := router.CallRW(ctx, bucketID, "echo", args, routerOpts)
		require.NoError(t, err, "router.CallRW with no err")

		var resViaVshard interface{}
		var resDirect interface{}

		err = resp.GetTransparent(&resViaVshard)
		require.NoError(t, err, "GetTransparent with no err")

		var rsOpts vshardrouter.ReplicasetCallOpts

		err = rs.CallAsync(ctx, rsOpts, "echo", args).GetTyped(&resDirect)
		require.NoError(t, err, "rs.CallAsync.GetTyped with no error")

		require.Equalf(t, resDirect, resViaVshard, "resDirect != resViaVshard on argLen %d", argLen)
	}
}
