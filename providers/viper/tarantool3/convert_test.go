package tarantool3

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestConvert_ShardingWeight(t *testing.T) {
	weight := func(w float64) *float64 { return &w }
	instances := map[string]Instance{
		"i": {IProto: IProto{Listen: []Listen{{URI: "127.0.0.1:3301"}}}},
	}
	weights := func(cfg Config) map[string]float64 {
		t.Helper()
		topology, err := cfg.Convert()
		require.NoError(t, err)
		result := make(map[string]float64, len(topology))
		for rs := range topology {
			result[rs.Name] = rs.Weight
		}
		return result
	}

	t.Run("default", func(t *testing.T) {
		cfg := Config{Groups: Group{Storages: &Storages{
			Replicasets: map[string]Replicaset{"a": {Instances: instances}},
		}}}
		require.Equal(t, map[string]float64{"a": 1}, weights(cfg))
	})

	t.Run("most specific scope wins", func(t *testing.T) {
		cfg := Config{
			Sharding: Sharding{Weight: weight(2)},
			Groups: Group{Storages: &Storages{
				Sharding: Sharding{Weight: weight(3)},
				Replicasets: map[string]Replicaset{
					"group":      {Instances: instances},
					"replicaset": {Sharding: Sharding{Weight: weight(4)}, Instances: instances},
					"zero":       {Sharding: Sharding{Weight: weight(0)}, Instances: instances},
				},
			}},
		}
		require.Equal(t, map[string]float64{"group": 3, "replicaset": 4, "zero": 0}, weights(cfg))
	})

	t.Run("global", func(t *testing.T) {
		cfg := Config{
			Sharding: Sharding{Weight: weight(2)},
			Groups: Group{Storages: &Storages{
				Replicasets: map[string]Replicaset{"a": {Instances: instances}},
			}},
		}
		require.Equal(t, map[string]float64{"a": 2}, weights(cfg))
	})
}
