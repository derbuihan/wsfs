package fuse

import (
	"context"
	"syscall"
	"testing"

	"github.com/hanwen/go-fuse/v2/fs"
	"github.com/hanwen/go-fuse/v2/fuse"
)

type testDirStream struct {
	entries []fuse.DirEntry
	idx     int
	closed  bool
}

func (d *testDirStream) HasNext() bool {
	return d.idx < len(d.entries)
}

func (d *testDirStream) Next() (fuse.DirEntry, syscall.Errno) {
	if !d.HasNext() {
		return fuse.DirEntry{}, 0
	}
	e := d.entries[d.idx]
	d.idx++
	return e, 0
}

func (d *testDirStream) Close() {
	d.closed = true
}

type seekableDirStream struct {
	testDirStream
	lastOff uint64
	errno   syscall.Errno
}

func (s *seekableDirStream) Seekdir(ctx context.Context, off uint64) syscall.Errno {
	s.lastOff = off
	if int(off) <= len(s.entries) {
		s.idx = int(off)
	} else {
		s.idx = len(s.entries)
	}
	return s.errno
}

func TestDirStreamHandle_Readdirent_CreatorOnce(t *testing.T) {
	stream := &testDirStream{entries: []fuse.DirEntry{{Name: "a"}, {Name: "b"}}}
	calls := 0

	h := &dirStreamHandle{
		creator: func(ctx context.Context) (fs.DirStream, syscall.Errno) {
			calls++
			return stream, 0
		},
	}

	ctx := context.Background()
	entry, errno := h.Readdirent(ctx)
	if errno != 0 {
		t.Fatalf("unexpected errno: %d", errno)
	}
	if entry == nil || entry.Name != "a" {
		t.Fatalf("unexpected entry: %#v", entry)
	}

	entry, errno = h.Readdirent(ctx)
	if errno != 0 {
		t.Fatalf("unexpected errno: %d", errno)
	}
	if entry == nil || entry.Name != "b" {
		t.Fatalf("unexpected entry: %#v", entry)
	}

	entry, errno = h.Readdirent(ctx)
	if errno != 0 {
		t.Fatalf("unexpected errno at EOF: %d", errno)
	}
	if entry != nil {
		t.Fatalf("expected nil entry at EOF, got %#v", entry)
	}

	if calls != 1 {
		t.Fatalf("expected creator called once, got %d", calls)
	}
}

func TestDirStreamHandle_Readdirent_CreatorError(t *testing.T) {
	h := &dirStreamHandle{
		creator: func(ctx context.Context) (fs.DirStream, syscall.Errno) {
			return nil, syscall.EIO
		},
	}

	entry, errno := h.Readdirent(context.Background())
	if errno != syscall.EIO {
		t.Fatalf("expected EIO, got %d", errno)
	}
	if entry != nil {
		t.Fatalf("expected nil entry on error, got %#v", entry)
	}
}

func TestDirStreamHandle_Releasedir_ClosesStream(t *testing.T) {
	stream := &testDirStream{entries: []fuse.DirEntry{{Name: "a"}}}
	h := &dirStreamHandle{
		creator: func(ctx context.Context) (fs.DirStream, syscall.Errno) {
			return stream, 0
		},
	}

	_, _ = h.Readdirent(context.Background())
	h.Releasedir(context.Background(), 0)

	if !stream.closed {
		t.Fatal("expected stream to be closed")
	}
}

func TestDirStreamHandle_Seekdir_Seekable(t *testing.T) {
	stream := &seekableDirStream{testDirStream: testDirStream{entries: []fuse.DirEntry{{Name: "a"}, {Name: "b"}}}}
	h := &dirStreamHandle{
		creator: func(ctx context.Context) (fs.DirStream, syscall.Errno) {
			return stream, 0
		},
	}

	errno := h.Seekdir(context.Background(), 1)
	if errno != 0 {
		t.Fatalf("expected success, got %d", errno)
	}
	if stream.lastOff != 1 {
		t.Fatalf("expected lastOff=1, got %d", stream.lastOff)
	}
}

func TestDirStreamHandle_Seekdir_NotSupported(t *testing.T) {
	stream := &testDirStream{entries: []fuse.DirEntry{{Name: "a"}}}
	h := &dirStreamHandle{
		creator: func(ctx context.Context) (fs.DirStream, syscall.Errno) {
			return stream, 0
		},
	}

	errno := h.Seekdir(context.Background(), 0)
	if errno != syscall.ENOTSUP {
		t.Fatalf("expected ENOTSUP, got %d", errno)
	}
}
