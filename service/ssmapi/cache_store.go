package ssmapi

import (
	"context"
	"errors"
	"io"
	"os"
	"path/filepath"

	"github.com/sagernet/sing/service/filemanager"
)

// MaxCacheBytes bounds this credential-bearing service state independently of
// any panel's users or quota database.
const MaxCacheBytes = 16 << 20

// CacheStore is an instance-local deployment file adapter. Authentication and
// counter interpretation remain owned by this service.
type CacheStore interface {
	Read(context.Context) ([]byte, error)
	Write(context.Context, []byte) error
}

type cacheStoreKey struct{}

func WithCacheStore(ctx context.Context, store CacheStore) context.Context {
	return context.WithValue(ctx, cacheStoreKey{}, store)
}

type fileCacheStore struct {
	ctx  context.Context
	path string
}

func (s fileCacheStore) Read(ctx context.Context) ([]byte, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	f, err := filemanager.Open(s.ctx, filemanager.BasePath(s.ctx, s.path))
	if err != nil {
		return nil, err
	}
	defer f.Close()
	info, err := f.Stat()
	if err != nil || !info.Mode().IsRegular() || info.Size() > MaxCacheBytes {
		return nil, errors.New("SSM_CACHE_FILE_INVALID")
	}
	data, err := io.ReadAll(io.LimitReader(f, MaxCacheBytes+1))
	if err != nil || len(data) > MaxCacheBytes {
		return nil, errors.New("SSM_CACHE_FILE_INVALID")
	}
	return data, ctx.Err()
}

func (s fileCacheStore) Write(ctx context.Context, data []byte) error {
	if len(data) > MaxCacheBytes {
		return errors.New("SSM_CACHE_EXCESSIVE")
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	path := filemanager.BasePath(s.ctx, s.path)
	if err := filemanager.MkdirAll(s.ctx, filepath.Dir(path), 0o700); err != nil {
		return err
	}
	// Deployments requiring path/identity/atomic-publication guarantees inject
	// their owned CacheStore; the portable default retains filemanager routing.
	return filemanager.WriteFile(s.ctx, path, data, os.FileMode(0o600))
}
