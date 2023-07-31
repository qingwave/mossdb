package mossdb

import (
	"encoding/binary"
	"errors"
	"time"
)

// |------------------------------------------------------------|
// | Op | KeySize | ValSize | Timestamp | TTL | Key    | Val    |
// |------------------------------------------------------------|
// | 2  | 4       | 4       | 8         | 8   | []byte | []byte |
// |------------------------------------------------------------|

const RecordHeaderSize = 2 + 4 + 4 + 8 + 8

type Record struct {
	Op        uint16
	KeySize   uint32
	ValSize   uint32
	Timestamp uint64
	TTL       uint64
	Key       []byte
	Val       []byte
}

func (r *Record) Size() uint32 {
	return RecordHeaderSize + r.KeySize + r.ValSize
}

func (r *Record) Encode() ([]byte, error) {
	if r == nil || len(r.Key) == 0 {
		return nil, ErrInvalidRecord
	}
	buf := make([]byte, r.Size())
	binary.BigEndian.PutUint16(buf[:2], r.Op)
	binary.BigEndian.PutUint32(buf[2:6], r.KeySize)
	binary.BigEndian.PutUint32(buf[6:10], r.ValSize)
	binary.BigEndian.PutUint64(buf[10:18], r.Timestamp)
	binary.BigEndian.PutUint64(buf[18:26], r.TTL)
	copy(buf[RecordHeaderSize:RecordHeaderSize+r.KeySize], r.Key)
	copy(buf[RecordHeaderSize+r.KeySize:RecordHeaderSize+r.KeySize+r.ValSize], r.Val)
	return buf, nil
}

var (
	ErrInvalidRecord = errors.New("invalid record")
)

func Decode(data []byte) (*Record, error) {
	if len(data) < RecordHeaderSize {
		return nil, ErrInvalidRecord
	}

	ks := binary.BigEndian.Uint32(data[2:6])
	vs := binary.BigEndian.Uint32(data[6:10])
	if len(data) != int(RecordHeaderSize+ks+vs) {
		return nil, ErrInvalidRecord
	}

	return &Record{
		Op:        binary.BigEndian.Uint16(data[:2]),
		KeySize:   ks,
		ValSize:   vs,
		Timestamp: binary.BigEndian.Uint64(data[10:18]),
		TTL:       binary.BigEndian.Uint64(data[18:26]),
		Key:       data[RecordHeaderSize : RecordHeaderSize+ks],
		Val:       data[RecordHeaderSize+ks : RecordHeaderSize+ks+vs],
	}, nil
}

func NewRecord(opt *Op) *Record {
	key_b := []byte(opt.key)
	return &Record{
		Op:        uint16(opt.op),
		KeySize:   uint32(len(key_b)),
		ValSize:   uint32(len(opt.val)),
		Key:       key_b,
		Val:       opt.val,
		Timestamp: uint64(time.Now().Nanosecond()),
		TTL:       uint64(opt.ttl),
	}
}

func OpFromRecord(record *Record) *Op {
	return &Op{
		key: string(record.Key),
		val: record.Val,
		ttl: int64(record.TTL),
	}
}
