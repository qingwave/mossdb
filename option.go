package mossdb

import (
	"time"
)

type OpType uint16

const (
	GetOp OpType = iota + 1
	ModifyOp
	DeleteOp
	WatchOP
	ListOp
)

var opMap = map[OpType]string{
	GetOp:    "GET",
	ModifyOp: "MODIFY",
	DeleteOp: "DELETE",
	WatchOP:  "WATCH",
}

func (op OpType) String() string {
	return opMap[op]
}

type Op struct {
	op OpType

	key string
	end string
	val Val

	ttl int64

	prefix bool
	all    bool

	msg string

	watchSendTimeout time.Duration

	createdNotify bool
	updateNotify  bool
	deleteNotify  bool
}

func NewOption(op OpType, key string, val Val, opts ...Option) *Op {
	opt := &Op{key: key, op: op, val: val}
	for _, f := range opts {
		f(opt)
	}
	return opt
}

func getOption(key string, opts ...Option) Op {
	return *NewOption(GetOp, key, nil, opts...)
}

func listOption(opts ...Option) Op {
	return *NewOption(ListOp, "", nil, opts...)
}

func setOption(key string, val Val, opts ...Option) *Op {
	return NewOption(ModifyOp, key, val, opts...)
}

func deleteOption(key string, opts ...Option) *Op {
	return NewOption(DeleteOp, key, nil, opts...)
}

func watchOption(key string, opts ...Option) *Op {
	return NewOption(WatchOP, key, nil, opts...)
}

func (op *Op) IsGet() bool {
	return op.op == GetOp
}

func (op *Op) IsMutate() bool {
	return op.op == ModifyOp || op.op == DeleteOp
}

type Option func(*Op)

func WithTTL(ttl time.Duration) Option {
	return func(opt *Op) {
		opt.ttl = time.Now().Add(ttl).UnixNano()
	}
}

func WithPrefix() Option {
	return func(opt *Op) {
		opt.prefix = true
	}
}

func WithPrefixKey(key string) Option {
	return func(opt *Op) {
		opt.key = key
		opt.prefix = true
	}
}

func WithAll() Option {
	return func(opt *Op) {
		opt.all = true
	}
}

func WithCreateNotify() Option {
	return func(opt *Op) {
		opt.createdNotify = true
	}
}

func WithUpdateNotify() Option {
	return func(opt *Op) {
		opt.updateNotify = true
	}
}

func WithDeleteNotify() Option {
	return func(opt *Op) {
		opt.deleteNotify = true
	}
}

func WithMsg(msg string) Option {
	return func(opt *Op) {
		opt.msg = msg
	}
}

func WithWatchSendTimeout(timeout time.Duration) Option {
	return func(opt *Op) {
		opt.watchSendTimeout = timeout
	}
}
