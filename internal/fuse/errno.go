package fuse

import (
	"errors"
	iofs "io/fs"
	"strings"
	"syscall"

	"github.com/databricks/databricks-sdk-go/apierr"
)

type backendOp string

const (
	backendOpLookup    backendOp = "lookup"
	backendOpReadDir   backendOp = "readdir"
	backendOpRead      backendOp = "read"
	backendOpWrite     backendOp = "write"
	backendOpCreate    backendOp = "create"
	backendOpMkdir     backendOp = "mkdir"
	backendOpDelete    backendOp = "unlink"
	backendOpDeleteDir backendOp = "rmdir"
	backendOpRename    backendOp = "rename"
)

func (op backendOp) mapsConflictToExist() bool {
	switch op {
	case backendOpCreate, backendOpMkdir, backendOpRename:
		return true
	default:
		return false
	}
}

func errnoFromBackendError(op backendOp, err error) syscall.Errno {
	if err == nil {
		return 0
	}

	var errno syscall.Errno
	if errors.As(err, &errno) {
		return errno
	}

	var apiError *apierr.APIError
	if errors.As(err, &apiError) {
		switch apiError.ErrorCode {
		case "RESOURCE_DOES_NOT_EXIST", "NOT_FOUND":
			return syscall.ENOENT
		case "PERMISSION_DENIED", "UNAUTHENTICATED":
			return syscall.EACCES
		case "INVALID_PARAMETER_VALUE", "BAD_REQUEST", "MALFORMED_REQUEST":
			return syscall.EINVAL
		case "DIRECTORY_NOT_EMPTY":
			return syscall.ENOTEMPTY
		case "RESOURCE_ALREADY_EXISTS", "ALREADY_EXISTS":
			if op.mapsConflictToExist() {
				return syscall.EEXIST
			}
		}

		message := strings.ToLower(apiError.Message)
		if op == backendOpDeleteDir && strings.Contains(message, "is not empty") {
			return syscall.ENOTEMPTY
		}
		if (op == backendOpCreate || op == backendOpWrite) &&
			strings.Contains(message, "parent folder") &&
			strings.Contains(message, "does not exist") {
			return syscall.ENOENT
		}
	}

	switch {
	case errors.Is(err, iofs.ErrNotExist), apierr.IsMissing(err):
		return syscall.ENOENT
	case errors.Is(err, iofs.ErrPermission),
		errors.Is(err, apierr.ErrPermissionDenied),
		errors.Is(err, apierr.ErrUnauthenticated):
		return syscall.EACCES
	case errors.Is(err, iofs.ErrInvalid),
		errors.Is(err, apierr.ErrBadRequest),
		errors.Is(err, apierr.ErrInvalidState),
		errors.Is(err, apierr.ErrInvalidParameterValue):
		return syscall.EINVAL
	case op.mapsConflictToExist() &&
		(errors.Is(err, iofs.ErrExist) ||
			errors.Is(err, apierr.ErrAlreadyExists) ||
			errors.Is(err, apierr.ErrResourceAlreadyExists) ||
			errors.Is(err, apierr.ErrResourceConflict)):
		return syscall.EEXIST
	}

	return syscall.EIO
}
