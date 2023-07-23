package storage

import (
	"encoding/binary"
	"errors"
	"fmt"
	"io/ioutil"
	"os"
	"path/filepath"
	"strconv"
	"sync"
)

var (
	LogCorrupt      = errors.New("Log is corrupted")
	LogClosed       = errors.New("Log is closed")
	LogFileNotFound = errors.New("Log file not found")
	LogEOF          = errors.New("Log file EOF reached")
)

type Options struct {
	// Disables fsync after writes. High risk of cause data loss when there is a server crash
	NoSync               bool
	SegmentSize          int // Default: 20MB
	DirectoryPermissions os.FileMode
	FilePermissions      os.FileMode
}

var DefaultOptions = &Options{
	NoSync:               false,    // Fsync after every write
	SegmentSize:          20971520, // 20 MB log segment files.
	DirectoryPermissions: 0750,
	FilePermissions:      0640,
}

func (o *Options) validate() {
	if o.SegmentSize <= 0 {
		o.SegmentSize = DefaultOptions.SegmentSize
	}

	if o.DirectoryPermissions == 0 {
		o.DirectoryPermissions = DefaultOptions.DirectoryPermissions
	}

	if o.FilePermissions == 0 {
		o.FilePermissions = DefaultOptions.FilePermissions
	}

}

type Log struct {
	mu       sync.RWMutex
	path     string
	segments []*segment
	sfile    *os.File //tail segment file handle
	wbatch   Batch    //reusable write batch

	opts    Options
	closed  bool
	corrupt bool
}

type segment struct {
	path  string //path of the segment file
	index uint64 //first index of the segment
	cbuf  []byte //cached entries buffer
	cpos  []bpos //cached entries positions in buffer
}

type bpos struct {
	pos int //byte position
	end int //one byte past pos
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
	log := &Log{path: path, opts: *opts}
	if err := os.Mkdir(path, log.opts.DirectoryPermissions); err != nil {
		return nil, err
	}
	if err := log.load(); err != nil {
		return nil, err
	}
	return log, nil
}

func (l *Log) load() error {
	files, err := ioutil.ReadDir(l.path)
	if err != nil {
		return err
	}
	for _, file := range files {
		name := file.Name()
		if file.IsDir() || len(name) < 20 {
			continue
		}
		index, err := strconv.ParseUint(name[:20], 10, 64)
		// nothing to read
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
		//create a new log
		l.segments = append(l.segments, &segment{
			index: 1,
			path:  filepath.Join(l.path, segmentName(1)),
		})
		l.sfile, err = os.OpenFile(l.segments[0].path, os.O_CREATE|os.O_RDWR|os.O_TRUNC, l.opts.FilePermissions)
		return err
	}

	// open the last segment for appending
	lastSegment := l.segments[len(l.segments)-1]
	l.sfile, err = os.OpenFile(lastSegment.path, os.O_WRONLY, l.opts.FilePermissions)
	if err != nil {
		return err
	}

	// seek relative to the end
	if _, err := l.sfile.Seek(0, 2); err != nil {
		return err
	}

	// load the last segment entries
	if err := l.loadSegmentEntries(lastSegment); err != nil {
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
	if l.closed {
		if l.corrupt {
			return LogCorrupt
		}
		return LogClosed
	}

	if err := l.sfile.Sync(); err != nil {
		return err
	}
	l.closed = true
	if l.corrupt {
		return LogCorrupt
	}

	return nil
}

func (l *Log) Write(data []byte) error {
	l.mu.Lock()
	defer l.mu.Unlock()

	if l.corrupt {
		return LogCorrupt
	} else if l.closed {
		return LogClosed
	}

	l.wbatch.Clear()
	l.wbatch.Write(data)
	return l.writeBatch(&l.wbatch)
}

func (l *Log) appendEntry(dst []byte, data []byte) (out []byte, cpos bpos) {
	return appendBinaryEntry(dst, data)
}

func (l *Log) cycle() error {
	var err error
	if err = l.sfile.Sync(); err != nil {
		return err
	}
	if err = l.sfile.Close(); err != nil {
		return err
	}

	nidx := l.segments[len(l.segments)-1].index + 1
	s := &segment{
		index: nidx,
		path:  filepath.Join(l.path, segmentName(nidx)),
	}

	l.sfile, err = os.OpenFile(s.path, os.O_CREATE|os.O_RDWR|os.O_TRUNC, l.opts.FilePermissions)
	if err != nil {
		return nil
	}
	l.segments = append(l.segments, s)
	return nil
}

func appendBinaryEntry(dst []byte, data []byte) (out []byte, cpos bpos) {
	pos := len(dst)
	dst = appendUvarint(dst, uint64(len(data)))
	dst = append(dst, data...)
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

func (l *Log) WriteBatch(b *Batch) error {
	l.mu.Lock()
	defer l.mu.Unlock()

	if l.corrupt {
		return LogCorrupt
	} else if l.closed {
		return LogClosed
	}

	if len(b.datas) == 0 {
		return nil
	}

	return l.writeBatch(b)
}

func (l *Log) writeBatch(b *Batch) error {
	// load the tail segment
	s := l.segments[len(l.segments)-1]
	if len(s.cbuf) > l.opts.SegmentSize {
		// tail segment has reached capacity. Close it and create a new one.
		if err := l.cycle(); err != nil {
			return err
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
		if len(s.cbuf) >= l.opts.SegmentSize {
			// segment has reached capacity, cycle now
			if _, err := l.sfile.Write(s.cbuf[mark:]); err != nil {
				return err
			}
			if err := l.cycle(); err != nil {
				return err
			}
			s = l.segments[len(l.segments)-1]
			mark = 0
		}
		datas = datas[b.entries[i].size:]
	}
	if len(s.cbuf)-mark > 0 {
		if _, err := l.sfile.Write(s.cbuf[mark:]); err != nil {
			return err
		}
	}

	if !l.opts.NoSync {
		if err := l.sfile.Sync(); err != nil {
			return err
		}
	}

	b.Clear()
	return nil
}

// findSegment performs a bsearch on the segments
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
	data, err := ioutil.ReadFile(s.path)
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
		return 0, LogCorrupt
	}
	if uint64(len(data)-n) < size {
		return 0, LogCorrupt
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
		return nil, LogCorrupt
	} else if l.closed {
		return nil, LogClosed
	}
	if segment == 0 {
		return nil, LogFileNotFound
	}
	s, err := l.loadSegment(segment)
	if err != nil {
		return nil, err
	}

	if int(index) >= len(s.cpos) {
		return nil, LogEOF
	}
	cpos := s.cpos[index]
	edata := s.cbuf[cpos.pos:cpos.end]
	// binary read
	size, n := binary.Uvarint(edata)
	if n <= 0 {
		return nil, LogCorrupt
	}
	if uint64(len(edata)-n) < size {
		return nil, LogCorrupt
	}
	data = make([]byte, size)
	copy(data, edata[n:])
	return data, nil
}

func (l *Log) Sync() error {
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.corrupt {
		return LogCorrupt
	} else if l.closed {
		return LogClosed
	}
	return l.sfile.Sync()
}
