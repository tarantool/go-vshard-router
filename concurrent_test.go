package vshard_router_test

import (
	"context"
	"math/rand"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	vshardrouter "github.com/tarantool/go-vshard-router/v2"
)

type concurrentTopologyProvider struct {
	done   chan struct{}
	closed chan struct{}
	t      *testing.T
}

func (c *concurrentTopologyProvider) Init(tc vshardrouter.TopologyController) error {
	ctx := context.Background()

	var cfg = make(map[vshardrouter.ReplicasetInfo][]vshardrouter.InstanceInfo)
	for k, v := range topology {
		cfg[k] = v
	}

	err := tc.AddReplicasets(ctx, cfg)
	require.NoError(c.t, err)

	c.done = make(chan struct{})
	c.closed = make(chan struct{})

	added := cfg
	removed := make(map[vshardrouter.ReplicasetInfo][]vshardrouter.InstanceInfo)

	go func() {
		defer close(c.closed)
		//nolint:errcheck
		defer tc.AddReplicasets(ctx, removed)

		type actiont int

		const add actiont = 0
		const remove actiont = 1

		for {
			select {
			case <-c.done:
				return
			default:
			}

			canAdd := len(removed) > 0
			canRemove := len(added) > 0

			var action actiont

			switch {
			case canAdd && canRemove:
				//nolint:gosec
				action = actiont(rand.Int() % 2)
			case canAdd:
				action = add
			case canRemove:
				action = remove
			default:
				require.Failf(c.t, "unreachable case", "%v, %v", added, removed)
			}

			switch action {
			case add:
				var keys []vshardrouter.ReplicasetInfo
				for k := range removed {
					keys = append(keys, k)
				}
				//nolint:gosec
				key := keys[rand.Int()%len(keys)]

				added[key] = removed[key]
				delete(removed, key)

				_ = tc.AddReplicaset(ctx, key, added[key])
			case remove:
				var keys []vshardrouter.ReplicasetInfo
				for k := range added {
					keys = append(keys, k)
				}
				//nolint:gosec
				key := keys[rand.Int()%len(keys)]

				removed[key] = added[key]
				delete(added, key)

				_ = tc.RemoveReplicaset(ctx, key.UUID.String())
			default:
				require.Fail(c.t, "unreachable case")
			}
		}
	}()

	return nil
}

func (c *concurrentTopologyProvider) Close() {
	close(c.done)
	<-c.closed
}

func TestConncurrentTopologyChange(t *testing.T) {
	/* What we do:
	1) Addreplicaset + Removereplicaset by random in one goroutine
	2) Call ReplicaCall, MapRw and etc. in another goroutines
	*/

	// Don't run this parallel with other tests, because this test is heavy and used to detect data races.
	// Therefore this test may impact other ones.
	// t.Parallel()

	tc := &concurrentTopologyProvider{t: t}

	router, err := vshardrouter.NewRouter(context.Background(), vshardrouter.Config{
		TopologyProvider: tc,
		DiscoveryTimeout: 5 * time.Second,
		DiscoveryMode:    vshardrouter.DiscoveryModeOn,
		TotalBucketCount: totalBucketCount,
		User:             username,
	})

	require.Nil(t, err, "NewRouter finished successfully")

	wg := sync.WaitGroup{}

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	const concurrentCalls = 100
	callCntArr := make([]int, concurrentCalls)
	for i := 0; i < 100; i++ {
		i := i
		wg.Add(1)
		go func() {
			defer wg.Done()

			for {
				select {
				case <-ctx.Done():
					return
				default:
				}

				bucketID := randBucketID(totalBucketCount)
				args := []interface{}{"arg1"}

				callOpts := vshardrouter.CallOpts{}

				_, _ = router.Call(ctx, bucketID, vshardrouter.CallModeBRO, "echo", args, callOpts)
				callCntArr[i]++
			}
		}()
	}

	var mapCnt int
	wg.Add(1)
	go func() {
		defer wg.Done()

		for {
			select {
			case <-ctx.Done():
				return
			default:
			}

			args := []interface{}{"arg1"}
			_, _ = vshardrouter.RouterMapCallRW[interface{}](router, ctx, "echo", args, vshardrouter.RouterMapCallRWOptions{})
			mapCnt++
		}
	}()

	wg.Wait()

	var callCnt int
	for _, v := range callCntArr {
		callCnt += v
	}

	t.Logf("Call cnt=%d, map cnt=%d", callCnt, mapCnt)

	tc.Close()
}
