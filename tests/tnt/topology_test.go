package tnt

import (
	"context"
	"math/rand"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	vshardrouter "github.com/tarantool/go-vshard-router"
	"github.com/tarantool/go-vshard-router/providers/static"
)

func TestTopology(t *testing.T) {
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

	var rsInfo vshardrouter.ReplicasetInfo
	var insInfo vshardrouter.InstanceInfo
	for k, replicas := range cfg {
		if len(replicas) == 0 {
			continue
		}
		rsInfo = k
		//nolint:gosec
		insInfo = replicas[rand.Int()%len(replicas)]
	}

	// remove some random replicaset
	_ = router.RemoveReplicaset(ctx, rsInfo.UUID)
	// add it again
	err = router.AddReplicasets(ctx, map[vshardrouter.ReplicasetInfo][]vshardrouter.InstanceInfo{rsInfo: cfg[rsInfo]})
	require.Nil(t, err, "AddReplicasets finished successfully")

	// remove some random instance
	err = router.RemoveInstance(ctx, rsInfo.UUID, insInfo.Name)
	require.Nil(t, err, "RemoveInstance finished successfully")

	// add it again
	err = router.AddInstance(ctx, rsInfo.UUID, insInfo)
	require.Nil(t, err, "AddInstance finished successfully")
}
