package fds

import (
	"context"
	"crypto/md5"
	"fmt"
	"hash"
	"io"
	"os"
	"path"
	"strconv"
	"strings"
	"time"

	"github.com/XiaoMi/go-fds/fds"
	"github.com/cenkalti/backoff/v4"

	"github.com/restic/restic/internal/backend"
	"github.com/restic/restic/internal/backend/sema"
	"github.com/restic/restic/internal/debug"
	"github.com/restic/restic/internal/errors"
	"github.com/restic/restic/internal/restic"
)

// Backend stores data on an fds endpoint.
type Backend struct {
	client *fds.Client
	sem    sema.Semaphore
	cfg    Config
	backend.Layout
}

// make sure that *Backend implements backend.Backend
var _ restic.Backend = &Backend{}

const defaultLayout = "default"

func newClient(ctx context.Context, cfg Config) (*fds.Client, error) {
	clientConfiguration, err := fds.NewClientConfiguration(cfg.FdsEndpoint)
	if err != nil {
		return nil, errors.Wrap(err, "fds.newClient")
	}
	if cfg.FdsAccessKey == "" || cfg.FdsSecretKey == "" {
		return nil, errors.New("Make sure your authentication information is set")
	}
	client := fds.New(cfg.FdsAccessKey, cfg.FdsSecretKey, clientConfiguration)
	return client, nil
}

func open(ctx context.Context, cfg Config) (*Backend, error) {
	debug.Log("open, config %#v", cfg)
	client, err := newClient(ctx, cfg)
	if err != nil {
		return nil, errors.Wrap(err, "fds.open")
	}

	sem, err := sema.New(cfg.Connections)
	if err != nil {
		return nil, err
	}

	be := &Backend{
		client: client,
		sem:    sem,
		cfg:    cfg,
	}
	l, err := backend.ParseLayout(ctx, be, cfg.Layout, defaultLayout, cfg.Prefix)
	if err != nil {
		return nil, err
	}

	be.Layout = l

	return be, nil
}

// Open opens the fds backend at bucket and region. The bucket is created if it
// does not exist yet.
func Open(ctx context.Context, cfg Config) (restic.Backend, error) {
	return open(ctx, cfg)
}

// Create opens the fds backend at bucket and region and creates the bucket if
// it does not exist yet.
func Create(ctx context.Context, cfg Config) (restic.Backend, error) {
	be, err := open(ctx, cfg)
	if err != nil {
		return nil, errors.Wrap(err, "open")
	}
	found, err := be.client.DoesBucketExist(cfg.Bucket)
	if err != nil && be.IsAccessDenied(err) {
		// bucket probably found but access to this bucket deny
		debug.Log("IsAccessDenied(%v) returned err %v", cfg.Bucket, err)
		return nil, errors.Wrap(err, "client.IsAccessDenied")
	}

	if err != nil && be.IsBadRequest(err) {
		debug.Log("IsBadRequest(%v) returned err %v", cfg.Bucket, err)
		return nil, errors.Wrap(err, "client.IsBadRequest")

	}

	if err != nil {
		return nil, errors.Wrap(err, "unknown err when create")
	}

	if !found {
		return nil, errors.New("Bucket not Exists, please create it first")
	}
	return be, nil
}

// IsBadRequest returns true if the error is caused by bad request
func (be *Backend) IsBadRequest(err error) bool {
	debug.Log("IsAccessDenied(%T, %#v)", err, err)
	if e, ok := errors.Cause(err).(*fds.ServerError); ok && e.Code() == 400 {
		return true
	}
	return false
}

// IsAccessDenied returns true if the error is caused by Access Denied.
func (be *Backend) IsAccessDenied(err error) bool {
	debug.Log("IsAccessDenied(%T, %#v)", err, err)
	if e, ok := errors.Cause(err).(*fds.ServerError); ok && e.Code() == 403 {
		return true
	}

	return false
}

// IsNotExist returns true if the error is caused by a not existing file.
func (be *Backend) IsNotExist(err error) bool {
	if err == nil {
		return true
	}
	debug.Log("IsNotExist(%T, %#v)", err, err)
	if os.IsNotExist(errors.Cause(err)) {
		return true
	}
	if e, ok := errors.Cause(err).(*fds.ServerError); ok && e.Code() == 404 {
		return true
	}
	return false
}

// Join combines path components with slashes.
func (be *Backend) Join(p ...string) string {
	return path.Join(p...)
}

type fileInfo struct {
	name    string
	size    int64
	mode    os.FileMode
	modTime time.Time
	isDir   bool
}

func (fi fileInfo) Name() string       { return fi.name }    // base name of the file
func (fi fileInfo) Size() int64        { return fi.size }    // length in bytes for regular files; system-dependent for others
func (fi fileInfo) Mode() os.FileMode  { return fi.mode }    // file mode bits
func (fi fileInfo) ModTime() time.Time { return fi.modTime } // modification time
func (fi fileInfo) IsDir() bool        { return fi.isDir }   // abbreviation for Mode().IsDir()
func (fi fileInfo) Sys() interface{}   { return nil }        // underlying data source (can return nil)

// ReadDir returns the entries for a directory.
func (be *Backend) ReadDir(ctx context.Context, dir string) (list []os.FileInfo, err error) {
	debug.Log("ReadDir(%v)", dir)

	// make sure dir ends with a slash
	if dir[len(dir)-1] != '/' {
		dir += "/"
	}

	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	// debug.Log("using ListObjectsV1(%v)", be.cfg.ListObjectsV1)

	// for obj := range be.client.ListObjects(ctx, be.cfg.Bucket, minio.ListObjectsOptions{
	request := &fds.ListObjectsRequest{
		BucketName: be.cfg.Bucket,
		Prefix:     dir,
		Delimiter:  "/",
		MaxKeys:    1000,
	}
	listObj, err := be.client.ListObjectsWithContext(ctx, request)
	if err != nil {
		return nil, errors.Wrap(err, "ReadDir")
	}
	for {
		for _, obj := range listObj.ObjectSummaries {
			entry := fileInfo{
				name:    obj.ObjectName,
				size:    obj.Size,
				modTime: obj.LastModified,
			}
			list = append(list, entry)
		}
		if !listObj.Truncated {
			break
		}
		listObj, err = be.client.ListObjectsNextBatch(listObj)
		if err != nil {
			return nil, err
		}
	}
	return list, nil
}

func (be *Backend) Connections() uint {
	return be.cfg.Connections
}

// Location returns this backend's location (the bucket name).
func (be *Backend) Location() string {
	return be.Join(be.cfg.Bucket, be.cfg.Prefix)
}

// Hasher may return a hash function for calculating a content hash for the backend
func (be *Backend) Hasher() hash.Hash {
	return md5.New()
}

// Path returns the path in the bucket that is used for this backend.
func (be *Backend) Path() string {
	return be.cfg.Prefix
}

// HasAtomicReplace returns whether Save() can atomically replace files
func (be *Backend) HasAtomicReplace() bool {
	return true
}

// Save stores data in the backend at the handle.
func (be *Backend) Save(ctx context.Context, h restic.Handle, rd restic.RewindReader) error {
	debug.Log("Save %v", h)

	if err := h.Valid(); err != nil {
		return backoff.Permanent(err)
	}

	objName := be.Filename(h)

	be.sem.GetToken()
	defer be.sem.ReleaseToken()

	debug.Log("PutObject(%v, %v, %v)", be.cfg.Bucket, objName, rd.Length())
	request := &fds.PutObjectRequest{
		BucketName:    be.cfg.Bucket,
		ObjectName:    objName,
		ContentType:   "application/octet-stream",
		Data:          rd,
		ContentMd5:    fmt.Sprintf("%x", rd.Hash()),
		ContentLength: int(rd.Length()),
	}
	resp, err := be.client.PutObjectWithContext(ctx, request)
	// 检查err and resp.length

	debug.Log("%v -> %v bytes, err %#v: %v", objName, err, err)
	debug.Log("%v -> %v", objName, resp)
	return errors.Wrap(err, "client.PutObject")
}

// wrapReader wraps an io.ReadCloser to run an additional function on Close.
type wrapReader struct {
	io.ReadCloser
	f func()
}

func (wr wrapReader) Close() error {
	err := wr.ReadCloser.Close()
	wr.f()
	return err
}

// Load runs fn with a reader that yields the contents of the file at h at the
// given offset.
func (be *Backend) Load(ctx context.Context, h restic.Handle, length int, offset int64, fn func(rd io.Reader) error) error {
	return backend.DefaultLoad(ctx, h, length, offset, be.openReader, fn)
}

func (be *Backend) openReader(ctx context.Context, h restic.Handle, length int, offset int64) (io.ReadCloser, error) {
	debug.Log("Load %v, length %v, offset %v from %v", h, length, offset, be.Filename(h))
	if err := h.Valid(); err != nil {
		return nil, backoff.Permanent(err)
	}

	if offset < 0 {
		return nil, errors.New("offset is negative")
	}

	if length < 0 {
		return nil, errors.Errorf("invalid length %d", length)
	}

	objName := be.Filename(h)
	var openRange string
	if length > 0 {
		openRange = fmt.Sprintf("bytes=%d-%d", offset, offset+int64(length)-1)
	} else if offset > 0 {
		openRange = fmt.Sprintf("bytes=%d-%d", offset, 0)
	}

	be.sem.GetToken()
	// coreClient := minio.Core{Client: be.client}

	request := &fds.GetObjectRequest{
		BucketName: be.cfg.Bucket,
		ObjectName: objName,
		Range:      openRange,
	}

	body, err := be.client.GetObjectWithContext(ctx, request)
	// rd, _, _, err := coreClient.GetObject(ctx, be.cfg.Bucket, objName, opts)
	if err != nil {
		be.sem.ReleaseToken()
		return nil, err
	}

	closeRd := wrapReader{
		ReadCloser: body,
		f: func() {
			debug.Log("Close()")
			be.sem.ReleaseToken()
		},
	}

	return closeRd, err
}

// Stat returns information about a blob.
func (be *Backend) Stat(ctx context.Context, h restic.Handle) (bi restic.FileInfo, err error) {
	debug.Log("%v", h)

	objName := be.Filename(h)

	be.sem.GetToken()
	obj, err := be.client.GetObjectMetadataWithContext(ctx, be.cfg.Bucket, objName)
	if err != nil {
		debug.Log("GetObject() err %v", err)
		be.sem.ReleaseToken()
		return restic.FileInfo{}, errors.Wrap(err, "client.GetObject")
	}

	// make sure that the object is closed properly.
	defer func() {
		be.sem.ReleaseToken()
	}()

	objSize := obj.Get("x-xiaomi-meta-content-length")
	FISize, err := strconv.ParseInt(objSize, 10, 64)

	if err != nil {
		debug.Log("Stat() err %v", err)
		return restic.FileInfo{}, errors.Wrap(err, "Stat")
	}

	return restic.FileInfo{Size: FISize, Name: h.Name}, nil
}

// Test returns true if a blob of the given type and name exists in the backend.
func (be *Backend) Test(ctx context.Context, h restic.Handle) (bool, error) {
	found := false
	objName := be.Filename(h)

	be.sem.GetToken()
	found, err := be.client.DoesObjectExistWithContext(ctx, be.cfg.Bucket, objName)
	be.sem.ReleaseToken()
	// If error, then not found
	return found, err
}

// Remove removes the blob with the given name and type.
func (be *Backend) Remove(ctx context.Context, h restic.Handle) error {
	objName := be.Filename(h)

	be.sem.GetToken()
	err := be.client.DeleteObjectWithContext(ctx, be.cfg.Bucket, objName)
	be.sem.ReleaseToken()

	debug.Log("Remove(%v) at %v -> err %v", h, objName, err)

	if be.IsNotExist(err) {
		err = nil
	}

	return errors.Wrap(err, "client.RemoveObject")
}

// List runs fn for each file in the backend which has the type t. When an
// error occurs (or fn returns an error), List stops and returns it.
func (be *Backend) List(ctx context.Context, t restic.FileType, fn func(restic.FileInfo) error) error {
	debug.Log("listing %v", t)

	// prefix, recursive := be.Basedir(t)
	prefix, _ := be.Basedir(t)

	// make sure prefix ends with a slash
	if !strings.HasSuffix(prefix, "/") {
		prefix += "/"
	}

	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	request := &fds.ListObjectsRequest{
		BucketName: be.cfg.Bucket,
		Prefix:     prefix,
		MaxKeys:    1000,
	}
	listObj, err := be.client.ListObjectsWithContext(ctx, request)
	if err != nil {
		return errors.Wrap(err, "List")
	}
	for {
		for _, obj := range listObj.ObjectSummaries {
			// m := strings.TrimPrefix(obj.ObjectName, prefix)
			fi := restic.FileInfo{
				Name: path.Base(obj.ObjectName),
				Size: obj.Size,
			}
			if ctx.Err() != nil {
				return ctx.Err()
			}
			err := fn(fi)
			if err != nil {
				return err
			}
			if ctx.Err() != nil {
				return ctx.Err()
			}
		}
		if !listObj.Truncated {
			break
		}
		listObj, err = be.client.ListObjectsNextBatch(listObj)
		if err != nil {
			return err
		}
	}
	return ctx.Err()
}

// Remove keys for a specified backend type.
func (be *Backend) removeKeys(ctx context.Context, t restic.FileType) error {
	return be.List(ctx, restic.PackFile, func(fi restic.FileInfo) error {
		return be.Remove(ctx, restic.Handle{Type: t, Name: fi.Name})
	})
}

// Delete removes all restic keys in the bucket. It will not remove the bucket itself.
func (be *Backend) Delete(ctx context.Context) error {
	alltypes := []restic.FileType{
		restic.PackFile,
		restic.KeyFile,
		restic.LockFile,
		restic.SnapshotFile,
		restic.IndexFile}

	for _, t := range alltypes {
		err := be.removeKeys(ctx, t)
		if err != nil {
			return nil
		}
	}

	return be.Remove(ctx, restic.Handle{Type: restic.ConfigFile})
}

// Close does nothing
func (be *Backend) Close() error { return nil }

// Rename moves a file based on the new layout l.
func (be *Backend) Rename(ctx context.Context, h restic.Handle, l backend.Layout) error {
	debug.Log("Rename %v to %v", h, l)
	oldname := be.Filename(h)
	newname := l.Filename(h)

	if oldname == newname {
		debug.Log("  %v is already renamed", newname)
		return nil
	}

	debug.Log("  %v -> %v", oldname, newname)
	err := be.client.RenameObjectWithContext(ctx, be.cfg.Bucket, oldname, newname)
	if err != nil && be.IsNotExist(err) {
		debug.Log("copy failed: %v, seems to already have been renamed", err)
		return nil
	}

	if err != nil {
		debug.Log("copy failed: %v", err)
		return err
	}
	return nil
}
