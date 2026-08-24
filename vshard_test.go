package vshard_router //nolint:revive

import (
	"math"
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

func TestRouter_BucketSet_Range(t *testing.T) {
	t.Parallel()

	tCases := []struct {
		Name     string
		BucketID uint64
		ErrMsg   string
	}{
		{
			Name:     "zero bucket id",
			BucketID: 0,
			ErrMsg:   "bucket id is out of range: 0 (total 10)",
		},
		{
			Name:     "greater than total bucket count",
			BucketID: testRouterUpperBound + 1,
			ErrMsg:   "bucket id is out of range: 11 (total 10)",
		},
		{
			Name:     "max uint64",
			BucketID: math.MaxUint64,
			ErrMsg:   "bucket id is out of range",
		},
		{
			Name:     "lower bound",
			BucketID: 1,
		},
		{
			Name:     "upper bound",
			BucketID: testRouterUpperBound,
		},
	}

	for _, tc := range tCases {
		t.Run(tc.Name, func(t *testing.T) {
			t.Parallel()

			r, nameToRs := testRouter(testRouterUpperBound, "rs_1")

			rs, err := r.BucketSet(tc.BucketID, "rs_1")

			if tc.ErrMsg != "" {
				require.Nil(t, rs)
				require.ErrorContains(t, err, tc.ErrMsg)
			} else {
				require.NoError(t, err)
				require.Same(t, nameToRs["rs_1"], rs)
				require.Same(t, nameToRs["rs_1"], r.getRouteMap().get(tc.BucketID))
			}
		})
	}
}

func TestRouter_BucketReset_Range(t *testing.T) {
	t.Parallel()

	const bucketID = uint64(1)

	tCases := []struct {
		Name          string
		ResetBucketID uint64
		WantReset     bool
	}{
		{
			Name:          "zero bucket id",
			ResetBucketID: 0,
		},
		{
			Name:          "greater than total bucket count",
			ResetBucketID: testRouterUpperBound + 1,
		},
		{
			Name:          "max uint64",
			ResetBucketID: math.MaxUint64,
		},
		{
			Name:          "the bucket itself",
			ResetBucketID: bucketID,
			WantReset:     true,
		},
	}

	for _, tc := range tCases {
		t.Run(tc.Name, func(t *testing.T) {
			t.Parallel()

			r, nameToRs := testRouter(testRouterUpperBound, "rs_1")

			routeMap := r.getRouteMap()
			routeMap.set(bucketID, nameToRs["rs_1"])

			require.NotPanics(t, func() {
				r.BucketReset(tc.ResetBucketID)
			})

			if tc.WantReset {
				require.Nil(t, routeMap.get(bucketID))
				return
			}

			require.Same(t, nameToRs["rs_1"], routeMap.get(bucketID))
		})
	}
}
