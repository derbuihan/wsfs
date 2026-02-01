package fuse

import (
	"context"
	"syscall"

	"github.com/hanwen/go-fuse/v2/fs"
	"github.com/hanwen/go-fuse/v2/fuse"
)

// dirStreamHandle adapts a DirStream to a FileHandle for OpendirHandle.
type dirStreamHandle struct {
	creator func(context.Context) (fs.DirStream, syscall.Errno)
	ds      fs.DirStream
}

func (d *dirStreamHandle) Releasedir(ctx context.Context, releaseFlags uint32) {
	if d.ds != nil {
		d.ds.Close()
	}
}

func (d *dirStreamHandle) Readdirent(ctx context.Context) (*fuse.DirEntry, syscall.Errno) {
	if d.ds == nil {
		var errno syscall.Errno
		d.ds, errno = d.creator(ctx)
		if errno != 0 {
			return nil, errno
		}
	}

	if !d.ds.HasNext() {
		return nil, 0
	}

	e, errno := d.ds.Next()
	return &e, errno
}

func (d *dirStreamHandle) Seekdir(ctx context.Context, off uint64) syscall.Errno {
	if d.ds == nil {
		var errno syscall.Errno
		d.ds, errno = d.creator(ctx)
		if errno != 0 {
			return errno
		}
	}

	if sd, ok := d.ds.(fs.FileSeekdirer); ok {
		return sd.Seekdir(ctx, off)
	}

	return syscall.ENOTSUP
}
