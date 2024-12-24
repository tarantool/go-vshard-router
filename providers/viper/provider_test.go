package viper_test

import (
	"context"
	"testing"

	"github.com/spf13/viper"
	"github.com/stretchr/testify/require"

	vprovider "github.com/tarantool/go-vshard-router/providers/viper"
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

func TestNewProviderTarantool3(t *testing.T) {
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
