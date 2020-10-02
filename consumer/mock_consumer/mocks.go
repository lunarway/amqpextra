// Code generated by MockGen. DO NOT EDIT.
// Source: github.com/makasim/amqpextra/consumer (interfaces: Connection,ConnectionReady,Channel)

// Package mock_consumer is a generated GoMock package.
package mock_consumer

import (
	reflect "reflect"

	gomock "github.com/golang/mock/gomock"
	consumer "github.com/makasim/amqpextra/consumer"
	amqp "github.com/streadway/amqp"
)

// MockConnection is a mock of Connection interface.
type MockConnection struct {
	ctrl     *gomock.Controller
	recorder *MockConnectionMockRecorder
}

// MockConnectionMockRecorder is the mock recorder for MockConnection.
type MockConnectionMockRecorder struct {
	mock *MockConnection
}

// NewMockConnection creates a new mock instance.
func NewMockConnection(ctrl *gomock.Controller) *MockConnection {
	mock := &MockConnection{ctrl: ctrl}
	mock.recorder = &MockConnectionMockRecorder{mock}
	return mock
}

// EXPECT returns an object that allows the caller to indicate expected use.
func (m *MockConnection) EXPECT() *MockConnectionMockRecorder {
	return m.recorder
}

// MockConnectionReady is a mock of ConnectionReady interface.
type MockConnectionReady struct {
	ctrl     *gomock.Controller
	recorder *MockConnectionReadyMockRecorder
}

// MockConnectionReadyMockRecorder is the mock recorder for MockConnectionReady.
type MockConnectionReadyMockRecorder struct {
	mock *MockConnectionReady
}

// NewMockConnectionReady creates a new mock instance.
func NewMockConnectionReady(ctrl *gomock.Controller) *MockConnectionReady {
	mock := &MockConnectionReady{ctrl: ctrl}
	mock.recorder = &MockConnectionReadyMockRecorder{mock}
	return mock
}

// EXPECT returns an object that allows the caller to indicate expected use.
func (m *MockConnectionReady) EXPECT() *MockConnectionReadyMockRecorder {
	return m.recorder
}

// Conn mocks base method.
func (m *MockConnectionReady) Conn() consumer.Connection {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "Conn")
	ret0, _ := ret[0].(consumer.Connection)
	return ret0
}

// Conn indicates an expected call of Conn.
func (mr *MockConnectionReadyMockRecorder) Conn() *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "Conn", reflect.TypeOf((*MockConnectionReady)(nil).Conn))
}

// NotifyClose mocks base method.
func (m *MockConnectionReady) NotifyClose() chan struct{} {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "NotifyClose")
	ret0, _ := ret[0].(chan struct{})
	return ret0
}

// NotifyClose indicates an expected call of NotifyClose.
func (mr *MockConnectionReadyMockRecorder) NotifyClose() *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "NotifyClose", reflect.TypeOf((*MockConnectionReady)(nil).NotifyClose))
}

// MockChannel is a mock of Channel interface.
type MockChannel struct {
	ctrl     *gomock.Controller
	recorder *MockChannelMockRecorder
}

// MockChannelMockRecorder is the mock recorder for MockChannel.
type MockChannelMockRecorder struct {
	mock *MockChannel
}

// NewMockChannel creates a new mock instance.
func NewMockChannel(ctrl *gomock.Controller) *MockChannel {
	mock := &MockChannel{ctrl: ctrl}
	mock.recorder = &MockChannelMockRecorder{mock}
	return mock
}

// EXPECT returns an object that allows the caller to indicate expected use.
func (m *MockChannel) EXPECT() *MockChannelMockRecorder {
	return m.recorder
}

// Close mocks base method.
func (m *MockChannel) Close() error {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "Close")
	ret0, _ := ret[0].(error)
	return ret0
}

// Close indicates an expected call of Close.
func (mr *MockChannelMockRecorder) Close() *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "Close", reflect.TypeOf((*MockChannel)(nil).Close))
}

// Consume mocks base method.
func (m *MockChannel) Consume(arg0, arg1 string, arg2, arg3, arg4, arg5 bool, arg6 amqp.Table) (<-chan amqp.Delivery, error) {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "Consume", arg0, arg1, arg2, arg3, arg4, arg5, arg6)
	ret0, _ := ret[0].(<-chan amqp.Delivery)
	ret1, _ := ret[1].(error)
	return ret0, ret1
}

// Consume indicates an expected call of Consume.
func (mr *MockChannelMockRecorder) Consume(arg0, arg1, arg2, arg3, arg4, arg5, arg6 interface{}) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "Consume", reflect.TypeOf((*MockChannel)(nil).Consume), arg0, arg1, arg2, arg3, arg4, arg5, arg6)
}

// NotifyCancel mocks base method.
func (m *MockChannel) NotifyCancel(arg0 chan string) chan string {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "NotifyCancel", arg0)
	ret0, _ := ret[0].(chan string)
	return ret0
}

// NotifyCancel indicates an expected call of NotifyCancel.
func (mr *MockChannelMockRecorder) NotifyCancel(arg0 interface{}) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "NotifyCancel", reflect.TypeOf((*MockChannel)(nil).NotifyCancel), arg0)
}

// NotifyClose mocks base method.
func (m *MockChannel) NotifyClose(arg0 chan *amqp.Error) chan *amqp.Error {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "NotifyClose", arg0)
	ret0, _ := ret[0].(chan *amqp.Error)
	return ret0
}

// NotifyClose indicates an expected call of NotifyClose.
func (mr *MockChannelMockRecorder) NotifyClose(arg0 interface{}) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "NotifyClose", reflect.TypeOf((*MockChannel)(nil).NotifyClose), arg0)
}