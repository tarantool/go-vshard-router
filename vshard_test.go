package vshard_router //nolint:revive

import (
	"encoding/binary"
	"fmt"
	"hash/crc32"
	"math"
	"math/rand/v2"
	"testing"

	"github.com/snksoft/crc"
	"github.com/stretchr/testify/require"
)

func TestRouter_RouterBucketIDStrCRC32(t *testing.T) {
	r := Router{
		cfg: Config{TotalBucketCount: uint64(256000)},
	}

	t.Run("new logic with current hash sum", func(t *testing.T) {
		require.Equal(t, uint64(103202), r.BucketIDStrCRC32("2707623829"))
	})
}

func TestRouter_RouterBucketCount(t *testing.T) {
	bucketCount := uint64(123)

	r := Router{
		cfg: Config{TotalBucketCount: bucketCount},
	}

	require.Equal(t, bucketCount, r.BucketCount())
}

func TestRouter_RouteMapClean(t *testing.T) {
	r := Router{
		cfg: Config{TotalBucketCount: 10},
	}

	require.NotPanics(t, func() {
		r.RouteMapClean()
	})
}

func FuzzCRCSnkIsNotNeededCheck(f *testing.F) {
	rnd := rand.NewPCG(1, 1_000_000_000)

	for i := range 16384 {
		data := make([]byte, i+1)
		for j := range data {
			data[j] = byte(rnd.Uint64() & 0xFF)
		}

		f.Add(data)
	}

	table := crc32.MakeTable(crc32.Castagnoli)

	f.Fuzz(func(t *testing.T, data []byte) {
		sfkcrc := crc.NewHash(&crc.Parameters{
			Width:      32,
			Polynomial: 0x1EDC6F41,
			FinalXor:   0x0,
			ReflectIn:  true,
			ReflectOut: true,
			Init:       0xFFFFFFFF,
		})

		sfkres := sfkcrc.CalculateCRC(data)
		stdres := uint64(crc32.Checksum(data, table) ^ math.MaxUint32)

		if sfkres != stdres {
			t.Errorf("stdcrc %x != sftkcrd %x", stdres, sfkres)
		}
	})
}

func BenchmarkStdVsSFK(b *testing.B) {
	sfkcrc := crc.NewHash(&crc.Parameters{
		Width:      32,
		Polynomial: 0x1EDC6F41,
		FinalXor:   0x0,
		ReflectIn:  true,
		ReflectOut: true,
		Init:       0xFFFFFFFF,
	})
	table := crc32.MakeTable(crc32.Castagnoli)

	type test struct {
		data []byte
	}

	rnd := rand.NewPCG(1, 1_000_000)
	tests := [][]byte{
		randBytes(rnd, 1),
		randBytes(rnd, 2),
		randBytes(rnd, 3),
		randBytes(rnd, 4),
		randBytes(rnd, 8),
		randBytes(rnd, 32),
		randBytes(rnd, 64),
		randBytes(rnd, 128),
		randBytes(rnd, 256),
		randBytes(rnd, 512),
		randBytes(rnd, 1024),
		randBytes(rnd, 16384),
		randBytes(rnd, 10*1024*1024),
	}

	var counter uint64
	for _, data := range tests {
		b.Run(fmt.Sprintf("std-%d-bytes", len(data)), func(b *testing.B) {
			for b.Loop() {
				counter += uint64(crc32.Checksum(data, table) ^ math.MaxUint32)
			}
		})

		b.Run(fmt.Sprintf("sfk-%d-bytes", len(data)), func(b *testing.B) {
			for b.Loop() {
				counter += sfkcrc.CalculateCRC(data)
			}
		})
	}
}

func randBytes(rnd rand.Source, n int) []byte {
	var res []byte

	for n >= 8 {
		n -= 8
		res = binary.LittleEndian.AppendUint64(res, rnd.Uint64())
	}

	for n > 0 {
		n--
		res = append(res, byte(rnd.Uint64()))
	}

	return res
}
