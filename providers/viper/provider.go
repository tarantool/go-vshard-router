package viper

import (
	"context"
	"fmt"

	"github.com/google/uuid"
	srcviper "github.com/spf13/viper"
	vshardrouter "github.com/tarantool/go-vshard-router/v2"
	"github.com/tarantool/go-vshard-router/v2/providers/viper/moonlibs"
	"github.com/tarantool/go-vshard-router/v2/providers/viper/tarantool3"
)

// Check that provider implements TopologyProvider interface
var _ vshardrouter.TopologyProvider = (*Provider)(nil)

type Provider struct {
	ctx context.Context

	v  *srcviper.Viper
	rs map[vshardrouter.ReplicasetInfo][]vshardrouter.InstanceInfo
}

type ConfigType int

const (
	ConfigTypeMoonlibs ConfigType = iota
	ConfigTypeTarantool3
)

type Convertable interface {
	Convert() (map[vshardrouter.ReplicasetInfo][]vshardrouter.InstanceInfo, error)
}

func NewProvider(ctx context.Context, v *srcviper.Viper, cfgType ConfigType) *Provider {
	if v == nil {
		panic("viper entity is nil")
	}

	var cfg Convertable

	switch cfgType {
	case ConfigTypeMoonlibs:
		cfg = &moonlibs.Config{}
	case ConfigTypeTarantool3:
		cfg = &tarantool3.Config{}
	default:
		panic("unknown config type")
	}

	err := v.Unmarshal(cfg)
	if err != nil {
		panic(err)
	}

	resultMap, err := cfg.Convert()
	if err != nil {
		panic(err)
	}

	return &Provider{ctx: ctx, v: v, rs: resultMap}
}

func (p *Provider) WatchChanges() *Provider {
	// todo
	return p
}

func (p *Provider) Topology() map[vshardrouter.ReplicasetInfo][]vshardrouter.InstanceInfo {
	return p.rs
}

func (p *Provider) Validate() error {
	if len(p.rs) < 1 {
		return fmt.Errorf("replicasets are empty")
	}

	for rs := range p.rs {
		// check replicaset name
		if rs.Name == "" {
			return fmt.Errorf("one of replicaset name is empty")
		}

		// check replicaset uuid
		if rs.UUID == uuid.Nil {
			return fmt.Errorf("one of replicaset uuid is empty")
		}
	}

	return nil
}

func (p *Provider) Init(c vshardrouter.TopologyController) error {
	return c.AddReplicasets(p.ctx, p.rs)
}

func (p *Provider) Close() {}
