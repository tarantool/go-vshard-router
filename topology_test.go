package vshard_router //nolint:revive

import (
	"context"
	"errors"
	"testing"

	"github.com/google/uuid"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
	"github.com/tarantool/go-tarantool/v2/pool"
	mockpool "github.com/tarantool/go-vshard-router/v2/mocks/pool"
)

func TestRouter_Topology(t *testing.T) {
	router := Router{}

	require.NotNil(t, router.Topology())
}

func TestController_AddInstance(t *testing.T) {
	ctx := context.Background()

	t.Run("no such replicaset", func(t *testing.T) {
		router := Router{
			cfg: Config{
				Loggerf: emptyLogfProvider,
			},
		}
		router.setEmptyNameToReplicaset()

		err := router.Topology().AddInstance(ctx, uuid.New().String(), InstanceInfo{
			Addr: "127.0.0.1:8060",
			Name: "instance_001",
		})
		require.True(t, errors.Is(err, ErrReplicasetNotExists))
	})

	t.Run("invalid instance info", func(t *testing.T) {
		router := Router{
			cfg: Config{
				Loggerf: emptyLogfProvider,
			},
		}
		router.setEmptyNameToReplicaset()

		err := router.Topology().AddInstance(ctx, uuid.New().String(), InstanceInfo{})
		require.True(t, errors.Is(err, ErrInvalidInstanceInfo))
	})
}

func TestController_RemoveInstance_NoSuchReplicaset(t *testing.T) {
	t.Parallel()
	ctx := context.Background()

	router := Router{
		cfg: Config{
			Loggerf: emptyLogfProvider,
		},
	}
	router.setEmptyNameToReplicaset()

	err := router.Topology().RemoveInstance(ctx, uuid.New().String(), "")
	require.True(t, errors.Is(err, ErrReplicasetNotExists))

}

func TestController_RemoveInstance_NoReplicasetNameProvided(t *testing.T) {
	t.Parallel()
	ctx := context.Background()

	instanceName := "instance_001"

	mp := mockpool.NewPooler(t)
	mp.On("GetInfo").Return(map[string]pool.ConnectionInfo{
		instanceName: {
			ConnectedNow: true,
		},
	})

	mp.On("Remove", mock.Anything).Return(nil)

	router := Router{
		cfg: Config{
			Loggerf: emptyLogfProvider,
		},
	}
	_ = router.swapNameToReplicaset(nil, &map[string]*Replicaset{
		"replicaset_1": {
			conn: mp,
		},
	})

	err := router.Topology().RemoveInstance(ctx, "", instanceName)
	require.NoError(t, err)

}

func TestController_RemoveReplicaset(t *testing.T) {
	t.Parallel()

	ctx := context.Background()

	uuidToRemove := uuid.New()
	mPool := mockpool.NewPooler(t)
	mPool.On("CloseGraceful").Return(nil)

	router := Router{
		cfg: Config{
			Loggerf: emptyLogfProvider,
		},
	}
	_ = router.swapNameToReplicaset(nil, &map[string]*Replicaset{
		uuidToRemove.String(): {conn: mPool},
	})

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
		cfg: Config{
			Loggerf: emptyLogfProvider,
		},
	}
	_ = router.swapNameToReplicaset(nil, &map[string]*Replicaset{
		alreadyExistingRsName: {conn: nil},
	})

	// Test that such replicaset already exists
	err := router.AddReplicaset(ctx, ReplicasetInfo{Name: alreadyExistingRsName}, []InstanceInfo{})
	require.Equalf(t, ErrReplicasetExists, err, "such replicaset must already exists")
}

func TestRouter_AddReplicaset_InvalidReplicaset(t *testing.T) {
	t.Parallel()

	ctx := context.TODO()

	alreadyExistingRsName := uuid.New().String()

	router := Router{
		cfg: Config{
			Loggerf: emptyLogfProvider,
		},
	}
	_ = router.swapNameToReplicaset(nil, &map[string]*Replicaset{
		alreadyExistingRsName: {conn: nil},
	})

	// Test that such replicaset already exists
	rsInfo := ReplicasetInfo{}

	err := router.AddReplicaset(ctx, rsInfo, []InstanceInfo{})
	require.Error(t, err)
	require.Equal(t, rsInfo.Validate().Error(), err.Error())
}
