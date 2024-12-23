package static

import (
	"context"
	"fmt"

	vshardrouter "github.com/tarantool/go-vshard-router"
)

// Check that provider implements TopologyProvider interface
var _ vshardrouter.TopologyProvider = (*Provider)(nil)

type Provider struct {
	rs map[vshardrouter.ReplicasetInfo][]vshardrouter.InstanceInfo
}

func NewProvider(rs map[vshardrouter.ReplicasetInfo][]vshardrouter.InstanceInfo) *Provider {
	if rs == nil {
		panic("rs must not be nil")
	}

	if len(rs) == 0 {
		panic("rs must not be empty")
	}

	return &Provider{rs: rs}
}

func (p *Provider) Validate() error {
	for rs := range p.rs {
		// check replicaset name
		if rs.Name == "" {
			return fmt.Errorf("one of replicaset name is empty")
		}
	}

	return nil
}

func (p *Provider) Init(c vshardrouter.TopologyController) error {
	return c.AddReplicasets(context.TODO(), p.rs)
}

func (p *Provider) Close() {}
