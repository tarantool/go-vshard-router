package viper_test

import (
	"context"
	"log"
	"net/url"
	"os"
	"testing"

	"github.com/spf13/viper"
	_ "github.com/spf13/viper/remote"
	"github.com/stretchr/testify/require"
	vprovider "github.com/tarantool/go-vshard-router/providers/viper"
	clientv3 "go.etcd.io/etcd/client/v3"
	"go.etcd.io/etcd/server/v3/embed"
)

func TestProvider_Close(t *testing.T) {
	require.NotPanics(t, (&vprovider.Provider{}).Close)
}

func TestNewProviderNilPanic(t *testing.T) {
	require.Panics(t, func() {
		vprovider.NewProvider(context.TODO(), nil, vprovider.ConfigTypeMoonlibs)
	})
}

func TestNewProviderDirect(t *testing.T) {
	ctx := context.TODO()
	v := viper.New()

	v.AddConfigPath("testdata/")
	v.SetConfigName("config-direct")
	v.SetConfigType("yaml")

	err := v.ReadInConfig()
	require.NoError(t, err)

	provider := vprovider.NewProvider(ctx, v, vprovider.ConfigTypeMoonlibs)

	anyProviderValidation(t, provider)
}

func TestNewProviderSub(t *testing.T) {
	ctx := context.TODO()
	v := viper.New()

	v.AddConfigPath("testdata/")
	v.SetConfigName("config-sub")
	v.SetConfigType("yaml")

	err := v.ReadInConfig()
	require.NoError(t, err)

	v = v.Sub("supbpath")

	provider := vprovider.NewProvider(ctx, v, vprovider.ConfigTypeMoonlibs)

	anyProviderValidation(t, provider)
}

func TestNewProvider_Tarantool3(t *testing.T) {
	ctx := context.TODO()
	v := viper.New()

	v.AddConfigPath("testdata/")
	v.SetConfigName("config-tarantool3")
	v.SetConfigType("yaml")

	err := v.ReadInConfig()
	require.NoError(t, err)

	provider := vprovider.NewProvider(ctx, v, vprovider.ConfigTypeTarantool3)

	anyProviderValidation(t, provider)
}

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

func TestNewProvider_ETCD3(t *testing.T) {
	ctx := context.TODO()

	config := embed.NewConfig()

	config.Name = "localhost"
	config.Dir = "/tmp/my-embedded-ectd-cluster"

	config.ListenPeerUrls = parseEtcdUrls([]string{"http://0.0.0.0:2380"})
	config.ListenClientUrls = parseEtcdUrls([]string{"http://0.0.0.0:2379"})
	config.AdvertisePeerUrls = parseEtcdUrls([]string{"http://localhost:2380"})
	config.AdvertiseClientUrls = parseEtcdUrls([]string{"http://localhost:2379"})
	config.InitialCluster = "localhost=http://localhost:2380"
	config.LogLevel = "panic"

	etcd, err := embed.StartEtcd(config)
	require.NoError(t, err)

	defer etcd.Close()

	client, err := clientv3.New(clientv3.Config{
		Endpoints: []string{"localhost:2379"},
	})
	require.NoError(t, err)

	defer client.Close()

	kv := clientv3.NewKV(client)
	key := "/myapp"

	cfgBts, err := os.ReadFile("testdata/config-tarantool3.yaml")
	require.NoError(t, err)

	_, err = kv.Put(ctx, key, string(cfgBts))
	require.NoError(t, err)

	getResponse, err := kv.Get(ctx, key)
	require.NoError(t, err)
	require.NotEmpty(t, getResponse.Kvs[0].Value)

	etcdViper := viper.New()
	err = etcdViper.AddRemoteProvider("etcd3", "http://127.0.0.1:2379", key)
	require.NoError(t, err)

	etcdViper.SetConfigType("yaml")
	err = etcdViper.ReadRemoteConfig()
	require.NoError(t, err)

	provider := vprovider.NewProvider(ctx, etcdViper, vprovider.ConfigTypeTarantool3)

	anyProviderValidation(t, provider)
}

func anyProviderValidation(t testing.TB, provider *vprovider.Provider) {
	// not empty
	require.NotNil(t, provider)

	topology := provider.Topology()
	// topology not empty
	require.NotNil(t, topology)
	// topology more than 0
	require.True(t, len(topology) > 0)

	// there are no empty replicates
	for _, instances := range topology {
		require.NotEmpty(t, instances)
	}
}
