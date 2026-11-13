package vshard_router // nolint: revive

import (
	"bytes"
	"errors"
	"testing"
	"time"

	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
	"github.com/tarantool/go-tarantool/v3"
	"github.com/tarantool/go-tarantool/v3/pool"
	mockpool "github.com/tarantool/go-vshard-router/v2/mocks/pool"
	"github.com/vmihailenco/msgpack/v5"
	"github.com/vmihailenco/msgpack/v5/msgpcode"
)

func TestVshardMode_String_NotEmpty(t *testing.T) {
	t.Parallel()
	require.NotEmpty(t, ReadMode.String())
	require.NotEmpty(t, WriteMode.String())
}

func TestRouter_RouterRouteAll(t *testing.T) {
	t.Parallel()
	var emptyRouter = &Router{
		cfg: Config{
			TotalBucketCount: uint64(10),
			Metrics:          emptyMetricsProvider,
		},
	}
	emptyRouter.setEmptyNameToReplicaset()
	m := emptyRouter.RouteAll()
	require.Empty(t, m)
}

func TestVshardStorageCallResponseProto_DecodeMsgpack_ProtocolViolation(t *testing.T) {
	t.Parallel()

	tCases := map[string]func() *bytes.Buffer{
		"0 arr len": func() *bytes.Buffer {
			buf := bytes.NewBuffer(nil)

			return buf
		},
		"one bool false": func() *bytes.Buffer {
			buf := bytes.NewBuffer(nil)
			buf.WriteByte(msgpcode.FixedArrayLow | byte(1))
			buf.WriteByte(msgpcode.False)

			return buf
		},
		"double": func() *bytes.Buffer {
			buf := bytes.NewBuffer(nil)
			buf.WriteByte(msgpcode.FixedArrayLow | byte(1))
			buf.WriteByte(msgpcode.Double)

			return buf
		},
	}

	for tCaseName, bufGenerator := range tCases {
		protoResp := vshardStorageCallResponseProto{}

		t.Run(tCaseName, func(t *testing.T) {
			buf := bufGenerator()
			err := protoResp.DecodeMsgpack(msgpack.NewDecoder(buf))
			require.Error(t, err)
		})
	}
}

func TestVshardStorageCallResponseProto_DecodeMsgpack_StorageCallError(t *testing.T) {
	t.Parallel()
	prepareBuf := func() *bytes.Buffer {
		buf := bytes.NewBuffer(nil)
		buf.WriteByte(msgpcode.FixedArrayLow | byte(2))
		buf.WriteByte(msgpcode.Nil)

		return buf
	}

	tCases := map[string]func() (*bytes.Buffer, StorageCallVShardError){
		"empty storage call error": func() (*bytes.Buffer, StorageCallVShardError) {
			buf := prepareBuf()
			e := StorageCallVShardError{}

			err := msgpack.NewEncoder(buf).Encode(e)
			require.NoError(t, err)

			return buf, e
		},

		"name": func() (*bytes.Buffer, StorageCallVShardError) {
			buf := prepareBuf()
			e := StorageCallVShardError{
				Name: "test",
			}

			err := msgpack.NewEncoder(buf).Encode(e)
			require.NoError(t, err)

			return buf, e
		},
	}

	for tCaseName, bufGenerator := range tCases {
		t.Run(tCaseName, func(t *testing.T) {
			t.Parallel()

			protoResp := vshardStorageCallResponseProto{}

			buf, storageErr := bufGenerator()

			err := protoResp.DecodeMsgpack(msgpack.NewDecoder(buf))
			require.NoError(t, err)

			require.NotNil(t, protoResp.VshardError)
			require.Nil(t, protoResp.CallResp.buf)
			require.Nil(t, protoResp.AssertError)

			require.EqualValues(t, storageErr, *protoResp.VshardError)
		})

	}
}

func TestVshardStorageCallResponseProto_DecodeMsgpack_AssertError(t *testing.T) {
	t.Parallel()
	prepareBuf := func() *bytes.Buffer {
		buf := bytes.NewBuffer(nil)
		buf.WriteByte(msgpcode.FixedArrayLow | byte(2))
		buf.WriteByte(msgpcode.False)

		return buf
	}

	tCases := map[string]func() (*bytes.Buffer, assertError){
		"empty assert call error": func() (*bytes.Buffer, assertError) {
			buf := prepareBuf()
			e := assertError{}

			err := msgpack.NewEncoder(buf).Encode(e)
			require.NoError(t, err)

			return buf, e
		},
	}

	for tCaseName, bufGenerator := range tCases {
		t.Run(tCaseName, func(t *testing.T) {
			t.Parallel()

			protoResp := vshardStorageCallResponseProto{}

			buf, storageErr := bufGenerator()

			err := protoResp.DecodeMsgpack(msgpack.NewDecoder(buf))
			require.NoError(t, err)

			require.NotNil(t, protoResp.AssertError)
			require.Nil(t, protoResp.VshardError)
			require.Nil(t, protoResp.CallResp.buf)

			require.EqualValues(t, storageErr, *protoResp.AssertError)
		})

	}
}

func TestVshardStorageCallResponseProto_DecodeMsgpack_GetNonTyped(t *testing.T) {
	t.Parallel()
	prepareBuf := func() *bytes.Buffer {
		buf := bytes.NewBuffer(nil)
		buf.WriteByte(msgpcode.FixedArrayLow | byte(2))
		buf.WriteByte(msgpcode.True)

		return buf
	}

	tCases := map[string]func() (*bytes.Buffer, []any){
		"one string": func() (*bytes.Buffer, []any) {
			buf := prepareBuf()
			val := []any{"test", "test"}

			err := msgpack.NewEncoder(buf).Encode(val)
			require.NoError(t, err)

			return buf, []any{val}
		},
	}

	for tCaseName, bufGenerator := range tCases {
		t.Run(tCaseName, func(t *testing.T) {
			t.Parallel()

			protoResp := vshardStorageCallResponseProto{}

			buf, respExpect := bufGenerator()

			err := protoResp.DecodeMsgpack(msgpack.NewDecoder(buf))
			require.NoError(t, err)

			require.Nil(t, protoResp.AssertError)
			require.Nil(t, protoResp.VshardError)
			require.NotNil(t, protoResp.CallResp.buf)

			resp, err := protoResp.CallResp.Get()
			require.NoError(t, err)

			require.Equal(t, respExpect, resp)
		})

	}
}

func BenchmarkVshardStorageCallResponseProto_DecodeMsgpack_Ok(b *testing.B) {
	// Skip in timer buffer creation information
	b.StopTimer()

	// We need a lot of different examples to avoid caching the data in CPU caches.
	const examplesCount = 100_000
	var bufBytesArr [][]byte

	for i := 0; i < examplesCount; i++ {
		buf := bytes.NewBuffer(nil)

		err := msgpack.NewEncoder(buf).Encode([]any{true, i})
		require.NoError(b, err)

		bufBytesArr = append(bufBytesArr, buf.Bytes())
	}

	// allocate a bytesReader and msgpackDecoder only once to eliminate memory allocation intervention to benchmark
	bytesReader := bytes.NewReader(nil)
	msgpackDecoder := msgpack.NewDecoder(nil)

	// detects errors as count to minimize `require` module intervention to benchmark
	var errCount uint64
	var err error

	b.StartTimer()

	for i := 0; i < b.N; i++ {
		// benchmark will include some redundant things such Reset call below, it is ok for us now.
		// NOTE: get a new bufBytes each time to flush out CPU caches
		bytesReader.Reset(bufBytesArr[i%examplesCount])
		msgpackDecoder.Reset(bytesReader)

		// benchmark core parts are below
		// - protoResp.DecodeMsgpack
		// - protoResp.CallResp.Get()
		protoResp := vshardStorageCallResponseProto{}
		err = protoResp.DecodeMsgpack(msgpackDecoder)
		if err != nil {
			errCount++
		}

		_, err = protoResp.CallResp.Get()
		if err != nil {
			errCount++
		}
	}
	b.StopTimer()

	require.True(b, errCount == 0)
	b.ReportAllocs()
}

func TestRouterMapCallRW_StorageRefTimeout(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name    string
		timeout time.Duration
		seconds float64
	}{
		{name: "default", seconds: 0.5},
		{name: "subsecond", timeout: 125 * time.Millisecond, seconds: 0.125},
		{name: "fractional", timeout: 1500 * time.Millisecond, seconds: 1.5},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			stopErr := errors.New("stop after storage_ref")
			mPool := mockpool.NewPooler(t)
			mPool.EXPECT().Do(mock.Anything, pool.ModeRW).RunAndReturn(func(req tarantool.Request, _ pool.Mode) tarantool.Future {
				var actual, expected bytes.Buffer
				require.NoError(t, req.Body(nil, msgpack.NewEncoder(&actual)))
				expectedReq := tarantool.NewCallRequest("vshard.storage._call").
					Args([]any{"storage_ref", int64(1), tc.seconds})
				require.NoError(t, expectedReq.Body(nil, msgpack.NewEncoder(&expected)))
				require.Equal(t, expected.Bytes(), actual.Bytes(), "storage_ref timeout must be encoded in seconds")
				return tarantool.NewFutureWithErr(req, stopErr)
			}).Once()
			mPool.EXPECT().Do(mock.Anything, pool.ModeRW).RunAndReturn(func(req tarantool.Request, _ pool.Mode) tarantool.Future {
				return tarantool.NewFutureWithErr(req, stopErr)
			}).Once()

			router := &Router{cfg: Config{TotalBucketCount: 1}}
			require.NoError(t, router.swapNameToReplicaset(nil, &map[string]*Replicaset{"storage": {conn: mPool}}))
			_, err := RouterMapCallRW[any](router, t.Context(), "echo", []any{}, RouterMapCallRWOptions{Timeout: tc.timeout})
			require.ErrorContains(t, err, stopErr.Error())
		})
	}
}
