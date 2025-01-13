package vshard_router //nolint:revive

import (
	"context"
	"errors"
	"testing"

	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
	mockpool "github.com/tarantool/go-vshard-router/mocks/pool"
)

func TestRouter_Topology(t *testing.T) {
	router := Router{}

	require.NotNil(t, router.Topology())
}

func TestController_AddInstance(t *testing.T) {
	ctx := context.Background()

	t.Run("no such replicaset", func(t *testing.T) {
		router := Router{
			nameToReplicaset: map[string]*Replicaset{},
			cfg: Config{
				Loggerf: emptyLogfProvider,
			},
		}

		err := router.Topology().AddInstance(ctx, uuid.New().String(), InstanceInfo{
			Addr: "127.0.0.1:8060",
			Name: "instance_001",
		})
		require.True(t, errors.Is(err, ErrReplicasetNotExists))
	})

	t.Run("invalid instance info", func(t *testing.T) {
		router := Router{
			nameToReplicaset: map[string]*Replicaset{},
			cfg: Config{
				Loggerf: emptyLogfProvider,
			},
		}

		err := router.Topology().AddInstance(ctx, uuid.New().String(), InstanceInfo{})
		require.True(t, errors.Is(err, ErrInvalidInstanceInfo))
	})
}

func TestController_RemoveInstance(t *testing.T) {
	ctx := context.Background()

	t.Run("no such replicaset", func(t *testing.T) {
		router := Router{
			nameToReplicaset: map[string]*Replicaset{},
			cfg: Config{
				Loggerf: emptyLogfProvider,
			},
		}

		err := router.Topology().RemoveInstance(ctx, uuid.New().String(), "")
		require.True(t, errors.Is(err, ErrReplicasetNotExists))
	})
}

func TestController_RemoveReplicaset(t *testing.T) {
	ctx := context.Background()

	uuidToRemove := uuid.New()
	mPool := mockpool.NewPool(t)
	mPool.On("CloseGraceful").Return(nil)

	router := Router{
		nameToReplicaset: map[string]*Replicaset{
			uuidToRemove.String(): {conn: mPool},
		},
		cfg: Config{
			Loggerf: emptyLogfProvider,
		},
	}

	t.Run("no such replicaset", func(t *testing.T) {
		t.Parallel()
		errs := router.Topology().RemoveReplicaset(ctx, uuid.New().String())
		require.True(t, errors.Is(errs[0], ErrReplicasetNotExists))
	})
	t.Run("successfully remove", func(t *testing.T) {
		t.Parallel()
		errs := router.Topology().RemoveReplicaset(ctx, uuidToRemove.String())
		require.Empty(t, errs)
	})
}

func TestRouter_AddReplicaset_AlreadyExists(t *testing.T) {
	ctx := context.TODO()

	alreadyExistingRsName := uuid.New().String()

	router := Router{
		nameToReplicaset: map[string]*Replicaset{
			alreadyExistingRsName: {conn: nil},
		},
		cfg: Config{
			Loggerf: emptyLogfProvider,
		},
	}

	// Test that such replicaset already exists
	err := router.AddReplicaset(ctx, ReplicasetInfo{Name: alreadyExistingRsName}, []InstanceInfo{})
	require.Equalf(t, ErrReplicasetExists, err, "such replicaset must already exists")
}
