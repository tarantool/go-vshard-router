package vshard_router //nolint:revive

import (
	"testing"

	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
)

// testRouter returns a router with an initialized route map and the
// given replicasets.
func testRouter(totalBucketCount uint64, rsNames ...string) (*Router, nameToReplicasetMap) {
	r := &Router{
		cfg: Config{
			TotalBucketCount: totalBucketCount,
			Loggerf:          emptyLogfProvider,
			Metrics:          emptyMetricsProvider,
		},
	}

	nameToRs := make(nameToReplicasetMap, len(rsNames))
	for _, rsName := range rsNames {
		nameToRs[rsName] = &Replicaset{info: ReplicasetInfo{Name: rsName, UUID: uuid.New()}}
	}

	r.nameToReplicaset.Store(&nameToRs)
	r.setEmptyRouteMap()

	return r, nameToRs
}

func TestRouter_RouterBucketIDStrCRC32(t *testing.T) {
	r := Router{
		cfg: Config{TotalBucketCount: uint64(256000)},
	}

	t.Run("new logic with current hash sum", func(t *testing.T) {
		require.Equal(t, uint64(103202), r.BucketIDStrCRC32("2707623829"))
	})
}

func TestRouter_RouterBucketCount(t *testing.T) {
	bucketCount := uint64(123)

	r := Router{
		cfg: Config{TotalBucketCount: bucketCount},
	}

	require.Equal(t, bucketCount, r.BucketCount())
}

func TestRouter_RouteMapClean(t *testing.T) {
	r := Router{
		cfg: Config{TotalBucketCount: 10},
	}

	require.NotPanics(t, func() {
		r.RouteMapClean()
	})
}

const testRouterUpperBound = uint64(10)
