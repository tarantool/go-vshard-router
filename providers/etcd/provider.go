package etcd

import (
	"context"
	"fmt"
	"path/filepath"

	"github.com/google/uuid"
	vshardrouter "github.com/tarantool/go-vshard-router/v2"
	"go.etcd.io/etcd/client/v2"
)

var (
	ErrNodesError     = fmt.Errorf("etcd nodes err")
	ErrClusterError   = fmt.Errorf("etcd cluster err")
	ErrInstancesError = fmt.Errorf("etcd instances err")
	ErrInvalidUUID    = fmt.Errorf("invalid uuid")
)

// Check that provider implements TopologyProvider interface
var _ vshardrouter.TopologyProvider = (*Provider)(nil)

type Provider struct {
	// ctx is root ctx of application
	ctx context.Context

	kapi           client.KeysAPI
	path           string
	routerTopology vshardrouter.TopologyController
}

type Config struct {
	EtcdConfig client.Config
	// Path for storages configuration in etcd for example /project/store/storage
	Path string
}

// NewProvider returns provider to etcd configuration
// Set here path to etcd storages config and etcd config
func NewProvider(ctx context.Context, cfg Config) (*Provider, error) {
	c, err := client.New(cfg.EtcdConfig)
	if err != nil {
		return nil, err
	}

	kapi := client.NewKeysAPI(c)

	return &Provider{
		ctx:  ctx,
		kapi: kapi,
		path: cfg.Path,
	}, nil
}

// mapCluster2Instances combines clusters with instances in map
func mapCluster2Instances(replicasets []vshardrouter.ReplicasetInfo,
	instances map[string][]*vshardrouter.InstanceInfo) map[vshardrouter.ReplicasetInfo][]vshardrouter.InstanceInfo {

	currentTopology := map[vshardrouter.ReplicasetInfo][]vshardrouter.InstanceInfo{}

	for _, replicasetInfo := range replicasets {
		var resInst []vshardrouter.InstanceInfo

		for _, inst := range instances[replicasetInfo.Name] {
			resInst = append(resInst, *inst)
		}

		currentTopology[replicasetInfo] = resInst
	}

	return currentTopology
}

// nolint:contextcheck
func (p *Provider) GetTopology() (map[vshardrouter.ReplicasetInfo][]vshardrouter.InstanceInfo, error) {
	resp, err := p.kapi.Get(context.TODO(), p.path, &client.GetOptions{Recursive: true})
	if err != nil {
		return nil, err
	}
	nodes := resp.Node.Nodes

	if nodes.Len() < 2 {
		return nil, fmt.Errorf("%w: etcd path %s subnodes <2; minimum 2 (/clusters & /instances)", ErrNodesError, p.path)
	}

	var replicasets []vshardrouter.ReplicasetInfo
	instances := map[string][]*vshardrouter.InstanceInfo{} // cluster name to instance info

	for _, node := range nodes {
		var err error

		switch filepath.Base(node.Key) {
		case "clusters":
			if len(node.Nodes) < 1 {
				return nil, fmt.Errorf("%w: etcd path %s has no clusters", ErrClusterError, node.Key)
			}

			for _, rsNode := range node.Nodes {
				replicaset := vshardrouter.ReplicasetInfo{}

				replicaset.Name = filepath.Base(rsNode.Key)

				for _, rsInfoNode := range rsNode.Nodes {
					switch filepath.Base(rsInfoNode.Key) {
					case "replicaset_uuid":
						replicaset.UUID, err = uuid.Parse(rsInfoNode.Value)
						if err != nil {
							return nil, fmt.Errorf("cant parse replicaset %s uuid %s", replicaset.Name, rsInfoNode.Value)
						}
					case "master":
						// TODO: now we dont support non master auto implementation
					default:
						continue
					}
				}

				replicasets = append(replicasets, replicaset)
			}
		case "instances":
			if len(node.Nodes) < 1 {
				return nil, fmt.Errorf("%w: etcd path %s has no instances", ErrInstancesError, node.Key)
			}

			for _, instanceNode := range node.Nodes {
				instanceName := filepath.Base(instanceNode.Key)

				instance := &vshardrouter.InstanceInfo{
					Name: instanceName,
				}

				for _, instanceInfoNode := range instanceNode.Nodes {
					switch filepath.Base(instanceInfoNode.Key) {
					case "cluster":
						instances[instanceInfoNode.Value] = append(instances[instanceInfoNode.Value], instance)
					case "box":
						for _, boxNode := range instanceInfoNode.Nodes {
							switch filepath.Base(boxNode.Key) {
							case "listen":
								instance.Addr = boxNode.Value
							case "instance_uuid":
								instance.UUID, err = uuid.Parse(boxNode.Value)
								if err != nil {
									return nil, fmt.Errorf("%w: cant parse for instance uuid %s",
										ErrInvalidUUID, boxNode.Value)
								}
							}
						}
					}
				}
			}
		default:
			continue
		}
	}

	if replicasets == nil {
		return nil, fmt.Errorf("empty replicasets")
	}

	currentTopology := mapCluster2Instances(replicasets, instances)

	return currentTopology, nil
}

func (p *Provider) Init(c vshardrouter.TopologyController) error {
	p.routerTopology = c

	topology, err := p.GetTopology()
	if err != nil {
		return err
	}

	return c.AddReplicasets(p.ctx, topology)
}

func (p *Provider) WatchChanges(ctx context.Context) {
	tc := p.routerTopology

	w := p.kapi.Watcher(p.path, &client.WatcherOptions{
		Recursive: true,
	})

	for {
		resp, err := w.Next(ctx)
		if err != nil {
			continue
		}

		t, err := p.GetTopology()
		if err != nil {
			continue
		}

	}
}

// Close must close connection, but etcd v2 client has no interfaces for this
func (p *Provider) Close() {}
