package etcd

import (
	"context"
	"errors"
	"fmt"
	"log"
	"net/url"
	"os"
	"testing"

	"github.com/stretchr/testify/require"
	vshardrouter "github.com/tarantool/go-vshard-router/v2"
	mocktopology "github.com/tarantool/go-vshard-router/v2/mocks/topology"
	"go.etcd.io/etcd/client/v2"
	"go.etcd.io/etcd/server/v3/embed"
)

func parseEtcdUrls(strs []string) []url.URL {
	urls := make([]url.URL, 0, len(strs))

	for _, str := range strs {
		u, err := url.Parse(str)
		if err != nil {
			log.Printf("Invalid url %s, error: %s", str, err.Error())
			continue

		}
		urls = append(urls, *u)
	}

	return urls
}

func TestNewProvider(t *testing.T) {
	ctx := context.Background()

	t.Run("no etcd endpoints provider error", func(t *testing.T) {
		t.Parallel()

		p, err := NewProvider(ctx, Config{EtcdConfig: client.Config{}})
		require.Error(t, err)
		require.Nil(t, p)
	})

	clientConfig := client.Config{Endpoints: []string{"http://127.0.0.1:2379"}}

	c, err := client.New(clientConfig)
	require.NoError(t, err)

	kapi := client.NewKeysAPI(c)
	require.NotNil(t, kapi)

	t.Run("provider creates ok", func(t *testing.T) {
		t.Parallel()

		p, err := NewProvider(ctx, Config{EtcdConfig: clientConfig})
		require.NoError(t, err)
		require.NotNil(t, p)
	})

	t.Run("provider invalid config error", func(t *testing.T) {
		invalidClientConfig := client.Config{Endpoints: []string{"http://0.0.0.0:23803"}}

		p, err := NewProvider(ctx, Config{EtcdConfig: invalidClientConfig})
		require.NoError(t, err)
		require.NotNil(t, p)

		_, err = p.GetTopology()
		require.Error(t, err)
	})

}

func TestProvider_GetTopology(t *testing.T) {
	ctx := context.Background()

	clientConfig := client.Config{Endpoints: []string{"http://127.0.0.1:2379"}}

	c, err := client.New(clientConfig)
	require.NoError(t, err)

	kapi := client.NewKeysAPI(c)
	require.NotNil(t, kapi)

	t.Run("nodes error", func(t *testing.T) {
		nodesErrorPath := "/no-nodes"

		p, err := NewProvider(ctx, Config{EtcdConfig: clientConfig, Path: nodesErrorPath})
		require.NoError(t, err)
		require.NotNil(t, p)

		_, err = kapi.Set(ctx, nodesErrorPath, "test", &client.SetOptions{})
		require.NoError(t, err)

		_, err = p.GetTopology()
		require.ErrorIs(t, err, ErrNodesError)
	})

	t.Run("no clusters or instances nodes error", func(t *testing.T) {
		nodesClusterErrPath := "/test-nodes-cluster-instances-error"

		p, err := NewProvider(ctx, Config{EtcdConfig: clientConfig, Path: nodesClusterErrPath})
		require.NoError(t, err)
		require.NotNil(t, p)

		_, _ = kapi.Set(ctx, nodesClusterErrPath, "", &client.SetOptions{Dir: true})

		_, err = kapi.Set(ctx, nodesClusterErrPath+"/clusters", "test", &client.SetOptions{})

		require.NoError(t, err)

		_, err = kapi.Set(ctx, nodesClusterErrPath+"/instances", "test", &client.SetOptions{})

		require.NoError(t, err)

		_, err = p.GetTopology()
		require.NotErrorIs(t, err, ErrNodesError)
		require.True(t, errors.Is(err, ErrClusterError) || errors.Is(err, ErrInstancesError))
	})

	t.Run("ok topology", func(t *testing.T) {
		/*tarantool:
		  userdb:
		    clusters:
		      userdb:
		        master: userdb_001
		        replicaset_uuid: 045e12d8-0001-0000-0000-000000000000
		    common:
		      box:
		        log_level: 5
		        memtx_memory: 268435456
		    instances:
		      userdb_001:
		        cluster: userdb
		        box:
		          instance_uuid: 045e12d8-0000-0001-0000-000000000000
		          listen: 10.0.1.11:3301
		      userdb_002:
		        cluster: userdb
		        box:
		          instance_uuid: 045e12d8-0000-0002-0000-000000000000
		          listen: 10.0.1.12:3302
		      userdb_003:
		        cluster: userdb
		        box:
		          instance_uuid: 045e12d8-0000-0003-0000-000000000000
		          listen: 10.0.1.13:3303
		*/

		dbName := "userdb"
		_, _ = kapi.Set(ctx, dbName, "", &client.SetOptions{Dir: true})

		p, err := NewProvider(ctx, Config{EtcdConfig: clientConfig, Path: dbName})
		require.NoError(t, err)

		// set root paths
		_, _ = kapi.Set(ctx, fmt.Sprintf("%s/%s", dbName, "clusters"), "", &client.SetOptions{Dir: true})
		_, _ = kapi.Set(ctx, fmt.Sprintf("%s/%s", dbName, "instances"), "", &client.SetOptions{Dir: true})
		// set cluster
		_, _ = kapi.Set(ctx, fmt.Sprintf("%s/%s/%s", dbName, "clusters", "userdb"), "", &client.SetOptions{Dir: true})

		_, _ = kapi.Set(ctx, fmt.Sprintf("%s/%s/%s", dbName, "instances", "userdb_001"), "", &client.SetOptions{Dir: true})
		_, _ = kapi.Set(ctx, fmt.Sprintf("%s/%s/%s/%s", dbName, "instances", "userdb_001", "cluster"), "userdb", &client.SetOptions{Dir: false})

		_, _ = kapi.Set(ctx, fmt.Sprintf("%s/%s/%s/%s", dbName, "instances", "userdb_001", "box"), "", &client.SetOptions{Dir: true})
		_, _ = kapi.Set(ctx, fmt.Sprintf("%s/%s/%s/%s/%s", dbName, "instances", "userdb_001", "box", "listen"), "10.0.1.13:3303", &client.SetOptions{Dir: false})

		topology, err := p.GetTopology()
		require.NoError(t, err)
		require.NotNil(t, topology)
	})

	t.Run("mapCluster2Instances", func(t *testing.T) {
		t.Parallel()

		tCases := []struct {
			name        string
			replicasets []vshardrouter.ReplicasetInfo
			instances   map[string][]*vshardrouter.InstanceInfo
			result      map[vshardrouter.ReplicasetInfo][]vshardrouter.InstanceInfo
		}{
			// test case 1
			{
				name: "1 rs and 1 instance for this rs",
				replicasets: []vshardrouter.ReplicasetInfo{
					{
						Name: "rs_1",
					},
				},
				instances: map[string][]*vshardrouter.InstanceInfo{
					"rs_1": {
						{
							Name: "inst_1_1",
						},
					},
				},
				result: map[vshardrouter.ReplicasetInfo][]vshardrouter.InstanceInfo{
					{
						Name: "rs_1",
					}: {
						{
							Name: "inst_1_1",
						},
					},
				},
			},
			// test case 2
			{
				name: "1 rs and 2 instance for this rs",
				replicasets: []vshardrouter.ReplicasetInfo{
					{
						Name: "rs_1",
					},
				},
				instances: map[string][]*vshardrouter.InstanceInfo{
					"rs_1": {
						{
							Name: "inst_1_1",
						},
						{
							Name: "inst_1_2",
						},
					},
				},
				result: map[vshardrouter.ReplicasetInfo][]vshardrouter.InstanceInfo{
					{
						Name: "rs_1",
					}: {
						{
							Name: "inst_1_1",
						},
						{
							Name: "inst_1_2",
						},
					},
				},
			},
		}

		for _, tCase := range tCases {
			t.Run(tCase.name, func(t *testing.T) {
				require.EqualValues(t, mapCluster2Instances(tCase.replicasets, tCase.instances), tCase.result)
			})
		}
	})
}

func runTestMain(m *testing.M) int {
	config := embed.NewConfig()

	config.Name = "localhost"
	config.Dir = "/tmp/my-embedded-ectd-cluster"

	config.ListenPeerUrls = parseEtcdUrls([]string{"http://0.0.0.0:2380"})
	config.ListenClientUrls = parseEtcdUrls([]string{"http://0.0.0.0:2379"})
	config.AdvertisePeerUrls = parseEtcdUrls([]string{"http://localhost:2380"})
	config.AdvertiseClientUrls = parseEtcdUrls([]string{"http://localhost:2379"})
	config.InitialCluster = "localhost=http://localhost:2380"
	config.LogLevel = "panic"

	// enable v2
	config.EnableV2 = true

	etcd, err := embed.StartEtcd(config)
	if err != nil {
		panic(err)
	}

	defer etcd.Close()

	return m.Run()
}

func TestProvider_Init(t *testing.T) {
	ctx := context.Background()

	clientConfig := client.Config{Endpoints: []string{"http://127.0.0.1:2379"}}

	p, err := NewProvider(ctx, Config{EtcdConfig: clientConfig})
	require.NoError(t, err)
	require.NotNil(t, p)

	t.Run("init with wrong provider returns topology error", func(t *testing.T) {
		tc := mocktopology.NewTopologyController(t)

		require.Error(t, p.Init(tc))
	})
}

func TestProvider_Close(t *testing.T) {
	ctx := context.Background()

	clientConfig := client.Config{Endpoints: []string{"http://127.0.0.1:2379"}}

	p, err := NewProvider(ctx, Config{EtcdConfig: clientConfig})
	require.NoError(t, err)
	require.NotNil(t, p)

	require.NotPanics(t, func() {
		p.Close()
	})
}

func TestMain(m *testing.M) {
	code := runTestMain(m)
	os.Exit(code)
}
