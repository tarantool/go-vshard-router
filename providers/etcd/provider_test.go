package etcd

import (
	"context"
	"errors"
	"log"
	"net/url"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
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

func TestProvider_Init(t *testing.T) {
	ctx := context.Background()
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
	require.NoError(t, err)

	defer etcd.Close()

	clientConfig := client.Config{Endpoints: []string{"http://127.0.0.1:2379"}}

	c, err := client.New(clientConfig)
	require.NoError(t, err)

	kapi := client.NewKeysAPI(c)
	require.NotNil(t, kapi)

	time.Sleep(1 * time.Second)

	t.Run("provider creates ok", func(t *testing.T) {
		t.Parallel()

		p, err := NewProvider(ctx, Config{EtcdConfig: clientConfig})
		require.NoError(t, err)
		require.NotNil(t, p)
	})

	t.Run("provider invalid config error", func(t *testing.T) {
		t.Parallel()

		invalidClientConfig := client.Config{Endpoints: []string{"http://0.0.0.0:23803"}}

		p, err := NewProvider(ctx, Config{EtcdConfig: invalidClientConfig})
		require.NoError(t, err)
		require.NotNil(t, p)

		_, err = p.GetTopology()
		require.Error(t, err)
	})

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

		_, err = kapi.Set(ctx, nodesClusterErrPath, "", &client.SetOptions{Dir: true})
		require.NoError(t, err)

		_, err = kapi.Set(ctx, nodesClusterErrPath+"/clusters", "test", &client.SetOptions{})

		require.NoError(t, err)

		_, err = kapi.Set(ctx, nodesClusterErrPath+"/instances", "test", &client.SetOptions{})

		require.NoError(t, err)

		_, err = p.GetTopology()
		require.NotErrorIs(t, err, ErrNodesError)
		require.True(t, errors.Is(err, ErrClusterError) || errors.Is(err, ErrInstancesError))
	})
}
