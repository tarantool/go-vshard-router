package vshard_router // nolint: revive

import (
	"testing"

	"github.com/stretchr/testify/require"
)

var emptyRouter = &Router{
	cfg: Config{
		TotalBucketCount: uint64(10),
		Loggerf:          emptyLogfProvider,
		Metrics:          emptyMetricsProvider,
	},
}

func TestVshardMode_String_NotEmpty(t *testing.T) {
	t.Parallel()
	require.NotEmpty(t, ReadMode.String())
	require.NotEmpty(t, WriteMode.String())
}

func TestRouter_RouterRouteAll(t *testing.T) {
	t.Parallel()
	m := emptyRouter.RouterRouteAll()
	require.Empty(t, m)
}
