package moonlibs

import (
	"testing"

	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
)

func TestConfig_Convert(t *testing.T) {
	t.Parallel()

	t.Run("name not empty", func(t *testing.T) {
		t.Parallel()
		cfg := Config{Topology: SourceTopologyConfig{
			Clusters: map[string]ClusterInfo{
				"cluster_1": {
					ReplicasetUUID: uuid.New().String(),
				},
			},
			Instances: map[string]InstanceInfo{
				"instance_1": {
					Cluster: "cluster_1",
					Box: struct {
						Listen       string `json:"listen,omitempty" yaml:"listen" mapstructure:"listen"`
						InstanceUUID string `yaml:"instance_uuid" mapstructure:"instance_uuid" json:"instanceUUID,omitempty"`
					}(struct {
						Listen       string
						InstanceUUID string
					}{Listen: "0.0.0.0:1111", InstanceUUID: uuid.New().String()}),
				},
			},
		}}

		m, err := cfg.Convert()
		require.NoError(t, err)

		for _, instances := range m {
			require.Len(t, instances, 1)
			require.NotEmpty(t, instances[0].Name)
		}
	})
}
