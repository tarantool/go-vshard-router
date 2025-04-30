package vshard_router //nolint:revive

import (
	"context"
	"fmt"

	"github.com/tarantool/go-iproto"
	"github.com/tarantool/go-tarantool/v2/pool"
)

// CallRequest helps you to create a call request object for execution
// by a Connection.
type CallRequest struct {
	ctx      context.Context
	fnc      string
	args     interface{}
	bucketID uint64
}

// CallResponse is a backwards-compatible structure with go-tarantool for easier replacement.
type CallResponse struct {
	resp VshardRouterCallResp
	err  error
}

// NewCallRequest returns a new empty CallRequest.
func NewCallRequest(function string) *CallRequest {
	req := new(CallRequest)
	req.fnc = function
	return req
}

// Do perform a request synchronously on the connection.
// It is important that the logic of this method is different from go-tarantool.
func (r *Router) Do(req *CallRequest, userMode pool.Mode) *CallResponse {
	ctx := req.ctx
	bucketID := req.bucketID
	resp := new(CallResponse)

	if req.fnc == "" {
		resp.err = fmt.Errorf("func name is empty")
		return resp
	}

	if req.args == nil {
		resp.err = fmt.Errorf("no request args")
		return resp
	}

	if req.bucketID == 0 {
		if r.cfg.BucketGetter == nil {
			resp.err = fmt.Errorf("bucket id for request is not set")
			return resp
		}

		bucketID = r.cfg.BucketGetter(ctx)
	}

	vshardMode := CallModeBRO

	// If the user says he prefers to do it on the master,
	// then he agrees that it will go to the replica, which means he will not write.
	if userMode == pool.RW {
		vshardMode = CallModeRW
	}

	resp.resp, resp.err = r.Call(ctx, bucketID, vshardMode, req.fnc, req.args, CallOpts{
		Timeout: r.cfg.RequestTimeout,
	})

	return resp
}

// Args sets the args for the eval request.
// Note: default value is empty.
func (req *CallRequest) Args(args interface{}) *CallRequest {
	req.args = args
	return req
}

// Context sets a passed context to the request.
func (req *CallRequest) Context(ctx context.Context) *CallRequest {
	req.ctx = ctx
	return req
}

// Type returns the type of the request, which is always IProto Call.
func (req *CallRequest) Type() iproto.Type {
	return iproto.IPROTO_CALL
}

// Ctx returns the context associated with the request,
// which can be used for cancellation, deadlines, etc.
func (req *CallRequest) Ctx() context.Context {
	return req.ctx
}

// Async returns false to the request return a response.
func (req *CallRequest) Async() bool {
	return false
}

// BucketID method that sets the bucketID for your request.
// You can ignore this parameter if you have a bucketGetter.
// However, this method has a higher priority.
func (req *CallRequest) BucketID(bucketID uint64) *CallRequest {
	req.bucketID = bucketID
	return req
}

// GetTyped waits synchronously for response and calls msgpack.Decoder.Decode(result) if no error happens.
func (resp *CallResponse) GetTyped(result interface{}) error {
	if resp.err != nil {
		return resp.err
	}

	return resp.resp.GetTyped(result)
}

// Get implementation now works synchronously for response.
// The interface was created purely for convenient migration to go-vshard-router from go-tarantool.
func (resp *CallResponse) Get() ([]interface{}, error) {
	if resp.err != nil {
		return nil, resp.err
	}

	return resp.resp.Get()
}
