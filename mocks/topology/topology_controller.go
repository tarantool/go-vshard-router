// Code generated by mockery v2.43.2. DO NOT EDIT.

package mocktopology

import (
	context "context"

	uuid "github.com/google/uuid"
	mock "github.com/stretchr/testify/mock"

	vshard_router "github.com/tarantool/go-vshard-router"
)

// TopologyController is an autogenerated mock type for the TopologyController type
type TopologyController struct {
	mock.Mock
}

// AddInstance provides a mock function with given fields: ctx, rsID, info
func (_m *TopologyController) AddInstance(ctx context.Context, rsID uuid.UUID, info vshard_router.InstanceInfo) error {
	ret := _m.Called(ctx, rsID, info)

	if len(ret) == 0 {
		panic("no return value specified for AddInstance")
	}

	var r0 error
	if rf, ok := ret.Get(0).(func(context.Context, uuid.UUID, vshard_router.InstanceInfo) error); ok {
		r0 = rf(ctx, rsID, info)
	} else {
		r0 = ret.Error(0)
	}

	return r0
}

// AddReplicaset provides a mock function with given fields: ctx, rsInfo, instances
func (_m *TopologyController) AddReplicaset(ctx context.Context, rsInfo vshard_router.ReplicasetInfo, instances []vshard_router.InstanceInfo) error {
	ret := _m.Called(ctx, rsInfo, instances)

	if len(ret) == 0 {
		panic("no return value specified for AddReplicaset")
	}

	var r0 error
	if rf, ok := ret.Get(0).(func(context.Context, vshard_router.ReplicasetInfo, []vshard_router.InstanceInfo) error); ok {
		r0 = rf(ctx, rsInfo, instances)
	} else {
		r0 = ret.Error(0)
	}

	return r0
}

// AddReplicasets provides a mock function with given fields: ctx, replicasets
func (_m *TopologyController) AddReplicasets(ctx context.Context, replicasets map[vshard_router.ReplicasetInfo][]vshard_router.InstanceInfo) error {
	ret := _m.Called(ctx, replicasets)

	if len(ret) == 0 {
		panic("no return value specified for AddReplicasets")
	}

	var r0 error
	if rf, ok := ret.Get(0).(func(context.Context, map[vshard_router.ReplicasetInfo][]vshard_router.InstanceInfo) error); ok {
		r0 = rf(ctx, replicasets)
	} else {
		r0 = ret.Error(0)
	}

	return r0
}

// RemoveInstance provides a mock function with given fields: ctx, rsID, instanceID
func (_m *TopologyController) RemoveInstance(ctx context.Context, rsID uuid.UUID, instanceID uuid.UUID) error {
	ret := _m.Called(ctx, rsID, instanceID)

	if len(ret) == 0 {
		panic("no return value specified for RemoveInstance")
	}

	var r0 error
	if rf, ok := ret.Get(0).(func(context.Context, uuid.UUID, uuid.UUID) error); ok {
		r0 = rf(ctx, rsID, instanceID)
	} else {
		r0 = ret.Error(0)
	}

	return r0
}

// RemoveReplicaset provides a mock function with given fields: ctx, rsID
func (_m *TopologyController) RemoveReplicaset(ctx context.Context, rsID uuid.UUID) []error {
	ret := _m.Called(ctx, rsID)

	if len(ret) == 0 {
		panic("no return value specified for RemoveReplicaset")
	}

	var r0 []error
	if rf, ok := ret.Get(0).(func(context.Context, uuid.UUID) []error); ok {
		r0 = rf(ctx, rsID)
	} else {
		if ret.Get(0) != nil {
			r0 = ret.Get(0).([]error)
		}
	}

	return r0
}

// NewTopologyController creates a new instance of TopologyController. It also registers a testing interface on the mock and a cleanup function to assert the mocks expectations.
// The first argument is typically a *testing.T value.
func NewTopologyController(t interface {
	mock.TestingT
	Cleanup(func())
}) *TopologyController {
	mock := &TopologyController{}
	mock.Mock.Test(t)

	t.Cleanup(func() { mock.AssertExpectations(t) })

	return mock
}
