package wal

import (
	"encoding/binary"
	"errors"
	"fmt"
	"hash/crc32"
	"os"
	"path/filepath"
	"strconv"
	"sync"
	"time"

	"github.com/gofrs/flock"
)

var (
	ErrCorrupt    = errors.New("log corrupt")
	ErrClosed     = errors.New("log closed")
	ErrInvaildCRC = errors.New("invaild crc")
	ErrNotFound   = errors.New("not found")
	ErrEOF        = errors.New("end of file reached")
	ErrFailedLock = errors.New("failed to get file lock")
)

const (
	crcSize  = crc32.Size
	lockFile = `flock`
)

type Options struct {
	// NoSync disables fsync after writes. This is less durable and puts the
	// log at risk of data loss when there's a server crash.
	NoSync       bool
	LoadAll      bool
	SyncDuration time.Duration
	SegmentSize  int // SegmentSize of each segment. Default is 20 MB.
	DirPerms     os.FileMode
	FilePerms    os.FileMode
}

func (o *Options) validate() {
	if o.SegmentSize <= 0 {
		o.SegmentSize = DefaultOptions.SegmentSize
	}

	if o.DirPerms == 0 {
		o.DirPerms = DefaultOptions.DirPerms
	}

	if o.FilePerms == 0 {
		o.FilePerms = DefaultOptions.FilePerms
	}

	if o.SyncDuration == 0 {
		o.SyncDuration = DefaultOptions.SyncDuration
	}
}

var DefaultOptions = &Options{
	NoSync:       false, // Fsync after every write
	SyncDuration: time.Second,
	SegmentSize:  20971520, // 20 MB log segment files.
	DirPerms:     0750,
	FilePerms:    0640,
}

// Log represents a append only log
type Log struct {
	mu       sync.RWMutex
	path     string     // absolute path to log directory
	segments []*segment // all known log segments
	sfile    *os.File   // tail segment file handle
	wbatch   Batch      // reusable write batch

	opts    Options
	closed  bool
	corrupt bool

	flock *flock.Flock
}

// segment represents a single segment file.
type segment struct {
	path  string // path of segment file
	index uint64 // first index of segment
	cbuf  []byte // cached entries buffer
	cpos  []bpos // cached entries positions in buffer
}

type bpos struct {
	pos int // byte position
	end int // one byte past pos
}

func Open(path string, opts *Options) (*Log, error) {
	if opts == nil {
		opts = DefaultOptions
	}
	opts.validate()

	var err error
	path, err = filepath.Abs(path)
	if err != nil {
		return nil, err
	}

	l := &Log{path: path, opts: *opts, flock: flock.New(filepath.Join(path, lockFile))}

	if err := os.MkdirAll(path, l.opts.DirPerms); err != nil {
		return nil, err
	}

	if err := l.tryLock(3, 100*time.Millisecond); err != nil {
		return nil, err
	}

	if err := l.load(); err != nil {
		return nil, err
	}

	return l, nil
}

func (l *Log) tryLock(times int, interval time.Duration) error {
	if l.flock == nil {
		return fmt.Errorf("flock is nil")
	}

	var ok bool
	var err error
	for i := 0; i < times; i++ {
		ok, err = l.flock.TryLock()
		if ok {
			return nil
		}
		if err != nil {
			return err
		}
		time.Sleep(interval)
	}

	return ErrFailedLock
}

func (l *Log) load() error {
	files, err := os.ReadDir(l.path)
	if err != nil {
		return err
	}

	for _, file := range files {
		name := file.Name()

		if file.IsDir() || len(name) < 20 {
			continue
		}

		index, err := strconv.ParseUint(name[:20], 10, 64)
		if err != nil || index == 0 {
			continue
		}

		if len(name) == 20 {
			l.segments = append(l.segments, &segment{
				index: index,
				path:  filepath.Join(l.path, name),
			})
		}
	}

	if len(l.segments) == 0 {
		// Create a new log
		l.segments = append(l.segments, &segment{
			index: 1,
			path:  filepath.Join(l.path, segmentName(1)),
		})
		l.sfile, err = os.OpenFile(l.segments[0].path, os.O_CREATE|os.O_RDWR|os.O_TRUNC, l.opts.FilePerms)
		return err
	}

	// Open the last segment for appending
	lseg := l.segments[len(l.segments)-1]
	l.sfile, err = os.OpenFile(lseg.path, os.O_WRONLY, l.opts.FilePerms)
	if err != nil {
		return err
	}

	if _, err := l.sfile.Seek(0, 2); err != nil {
		return err
	}

	// Load the last segment entries
	if err := l.loadSegmentEntries(lseg); err != nil {
		return err
	}

	return nil
}

func segmentName(index uint64) string {
	return fmt.Sprintf("%020d", index)
}

func (l *Log) Close() error {
	l.mu.Lock()
	defer l.mu.Unlock()

	defer l.flock.Unlock()

	if l.closed {
		if l.corrupt {
			return ErrCorrupt
		}
		return ErrClosed
	}
	if err := l.sfile.Sync(); err != nil {
		return err
	}
	if err := l.sfile.Close(); err != nil {
		return err
	}
	l.closed = true
	if l.corrupt {
		return ErrCorrupt
	}
	return nil
}

func (l *Log) Write(data []byte, opts ...LogOpition) (int, error) {
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.corrupt {
		return 0, ErrCorrupt
	} else if l.closed {
		return 0, ErrClosed
	}
	l.wbatch.Clear()
	l.wbatch.Write(data)
	return l.writeBatch(&l.wbatch, opts...)
}

func (l *Log) Flush() error {
	l.mu.Lock()
	defer l.mu.Unlock()

	if err := os.RemoveAll(l.path); err == os.ErrNotExist {
		return nil
	} else if err != nil {
		return err
	}

	if err := os.MkdirAll(l.path, l.opts.DirPerms); err != nil {
		return err
	}

	l.segments = make([]*segment, 0)

	return l.load()
}

func (l *Log) appendEntry(dst []byte, data []byte) (out []byte, cpos bpos) {
	return appendBinaryEntry(dst, data)
}

func (l *Log) cycle() error {
	if err := l.sfile.Sync(); err != nil {
		return err
	}
	if err := l.sfile.Close(); err != nil {
		return err
	}

	nidx := l.segments[len(l.segments)-1].index + 1
	s := &segment{
		index: nidx,
		path:  filepath.Join(l.path, segmentName(nidx)),
	}
	var err error
	l.sfile, err = os.OpenFile(s.path, os.O_CREATE|os.O_RDWR|os.O_TRUNC, l.opts.FilePerms)
	if err != nil {
		return err
	}
	l.segments = append(l.segments, s)
	return nil
}

// append data with crc
func appendBinaryEntry(dst []byte, data []byte) (out []byte, cpos bpos) {
	// data_size + data
	pos := len(dst)
	sum := crc32.ChecksumIEEE(data)
	buf := make([]byte, crcSize+len(data))
	binary.BigEndian.PutUint32(buf[:crcSize], sum)
	copy(buf[crcSize:], data)

	dst = appendUvarint(dst, uint64(len(buf)))
	dst = append(dst, buf...)
	return dst, bpos{pos, len(dst)}
}

func appendUvarint(dst []byte, x uint64) []byte {
	var buf [10]byte
	n := binary.PutUvarint(buf[:], x)
	dst = append(dst, buf[:n]...)
	return dst
}

type Batch struct {
	entries []batchEntry
	datas   []byte
}

type batchEntry struct {
	size int
}

func (b *Batch) Write(data []byte) {
	b.entries = append(b.entries, batchEntry{len(data)})
	b.datas = append(b.datas, data...)
}

func (b *Batch) Clear() {
	b.entries = b.entries[:0]
	b.datas = b.datas[:0]
}

type logOption struct {
	sync   bool
	atomic bool
}

type LogOpition func(*logOption)

func WithSync(opt *logOption) {
	opt.sync = true
}

func WithAtomic(opt *logOption) {
	opt.atomic = true
}

func (l *Log) WriteBatch(b *Batch, opts ...LogOpition) (int, error) {
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.corrupt {
		return 0, ErrCorrupt
	} else if l.closed {
		return 0, ErrClosed
	}
	if len(b.datas) == 0 {
		return 0, nil
	}
	return l.writeBatch(b, opts...)
}

func (l *Log) writeBatch(b *Batch, opts ...LogOpition) (int, error) {
	option := &logOption{}
	for _, f := range opts {
		f(option)
	}

	// load the tail segment
	s := l.segments[len(l.segments)-1]
	if len(s.cbuf) > l.opts.SegmentSize {
		// tail segment has reached capacity. Close it and create a new one.
		if err := l.cycle(); err != nil {
			return 0, err
		}
		s = l.segments[len(l.segments)-1]
	}

	mark := len(s.cbuf)
	datas := b.datas

	for i := 0; i < len(b.entries); i++ {
		data := datas[:b.entries[i].size]
		var cpos bpos
		s.cbuf, cpos = l.appendEntry(s.cbuf, data)
		s.cpos = append(s.cpos, cpos)
		if !option.atomic && len(s.cbuf) >= l.opts.SegmentSize {
			// segment has reached capacity, cycle now
			if n, err := l.sfile.Write(s.cbuf[mark:]); err != nil {
				return n, err
			}
			if err := l.cycle(); err != nil {
				return 0, err
			}
			s = l.segments[len(l.segments)-1]
			mark = 0
		}
		datas = datas[b.entries[i].size:]
	}

	if len(s.cbuf)-mark > 0 {
		if n, err := l.sfile.Write(s.cbuf[mark:]); err != nil {
			return n, err
		}
	}

	if !l.opts.NoSync && option.sync {
		if err := l.sfile.Sync(); err != nil {
			return 0, err
		}
	}

	b.Clear()
	return 0, nil
}

func (l *Log) RunSync(duration time.Duration) error {
	if l.opts.NoSync {
		return nil
	}

	if duration < time.Second {
		duration = time.Second
	}

	for {
		if err := l.Sync(); err != nil {
			return err
		}
		time.Sleep(duration)
	}
}

// findSegment performs a search on the segments
func (l *Log) findSegment(index uint64) int {
	i, j := 0, len(l.segments)
	for i < j {
		h := i + (j-i)/2
		if index >= l.segments[h].index {
			i = h + 1
		} else {
			j = h
		}
	}
	return i - 1
}

func (l *Log) loadSegmentEntries(s *segment) error {
	data, err := os.ReadFile(s.path)
	if err != nil {
		return err
	}
	ebuf := data
	var cpos []bpos
	var pos int
	for len(data) > 0 {
		var n int
		n, err = loadNextBinaryEntry(data)
		if err != nil {
			return err
		}
		data = data[n:]
		cpos = append(cpos, bpos{pos, pos + n})
		pos += n
	}
	s.cbuf = ebuf
	s.cpos = cpos
	return nil
}

func loadNextBinaryEntry(data []byte) (n int, err error) {
	// data_size + data
	size, n := binary.Uvarint(data)
	if n <= 0 {
		return 0, ErrCorrupt
	}
	if uint64(len(data)-n) < size {
		return 0, ErrCorrupt
	}
	return n + int(size), nil
}

func (l *Log) loadSegment(index uint64) (*segment, error) {
	// check the last segment first.
	lseg := l.segments[len(l.segments)-1]
	if index >= lseg.index {
		return lseg, nil
	}
	// find in the segment array
	idx := l.findSegment(index)
	s := l.segments[idx]
	if len(s.cpos) == 0 {
		// load the entries from cache
		if err := l.loadSegmentEntries(s); err != nil {
			return nil, err
		}
	}
	return s, nil
}

func (l *Log) Segments() int {
	return len(l.segments)
}

func (l *Log) Read(segment, index uint64) (data []byte, err error) {
	l.mu.RLock()
	defer l.mu.RUnlock()
	if l.corrupt {
		return nil, ErrCorrupt
	} else if l.closed {
		return nil, ErrClosed
	}
	if segment == 0 {
		return nil, ErrNotFound
	}
	s, err := l.loadSegment(segment)
	if err != nil {
		return nil, err
	}

	if int(index) >= len(s.cpos) {
		return nil, ErrEOF
	}
	cpos := s.cpos[index]
	edata := s.cbuf[cpos.pos:cpos.end]
	// binary read
	size, n := binary.Uvarint(edata)
	if n <= 0 {
		return nil, ErrCorrupt
	}

	if uint64(len(edata)-n) < size || size < crcSize {
		return nil, ErrCorrupt
	}

	sum := binary.BigEndian.Uint32(edata[n : n+crcSize])
	data = make([]byte, size-crcSize)
	copy(data, edata[n+crcSize:])

	if sum != crc32.ChecksumIEEE(data) {
		return nil, ErrInvaildCRC
	}

	return data, nil
}

func (l *Log) Sync() error {
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.corrupt {
		return ErrCorrupt
	} else if l.closed {
		return ErrClosed
	}
	return l.sfile.Sync()
}

func (l *Log) Truncate(n int) error {
	pos, err := l.sfile.Seek(-int64(n), 1)
	if err != nil {
		return err
	}

	return l.sfile.Truncate(int64(pos))
}
