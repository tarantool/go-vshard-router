# Viper Topology Provider

The Viper topology provider allows you to work with Tarantool configuration and retrieve topology from all sources, [which are supported by Viper in file formats](https://github.com/spf13/viper?tab=readme-ov-file#what-is-viper).

There are 2 supported configuration formats: moonlibs and tarantool3.
## Examples

### ETCD v3
```go
import (
	"context"
        "github.com/spf13/viper"
	_ "github.com/spf13/viper/remote" // dont forget to import remote pkg for viper remote
        vprovider "github.com/tarantool/go-vshard-router/providers/viper"
)

// ...
ctx := context.TODO()
key := "/myapp"
etcdViper := viper.New()
err = etcdViper.AddRemoteProvider("etcd3", "http://127.0.0.1:2379", key)
if err != nil {
	panic(err)
}

etcdViper.SetConfigType("yaml")
err = etcdViper.ReadRemoteConfig()
if err != nil {
panic(err)
}

provider := vprovider.NewProvider(ctx, etcdViper, vprovider.ConfigTypeTarantool3)

/// ...
```

Check more config examples in test dir or inside provider_test.go