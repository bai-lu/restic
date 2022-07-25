package fds

import (
	"path"
	"regexp"
	"strings"

	"github.com/restic/restic/internal/errors"
	"github.com/restic/restic/internal/options"
)

// Config contains all configuration necessary to connect to an fds compatible
// server.
type Config struct {
	FdsEndpoint  string
	FdsAccessKey string
	FdsSecretKey string
	Bucket       string
	Prefix       string
	Connections  uint   `option:"connections" help:"set a limit for the number of concurrent connections (default: 128)"`
	Layout       string `option:"layout" help:"use this backend layout (default: auto-detect)"`
}

// NewConfig returns a new Config with the default values filled in.
func NewConfig() Config {
	return Config{
		Connections: 128,
		Layout:      "default",
	}
}

func init() {
	options.Register("fds", Config{})
}

var bucketName = regexp.MustCompile("^[a-zA-Z0-9-]+$")

// checkBucketName tests the bucket name against the rules at https://github.com/XiaoMi/go-fds
func checkBucketName(name string) error {
	if name == "" {
		return errors.New("bucket name is empty")
	}

	if len(name) < 3 {
		return errors.New("bucket name is too short")
	}

	if len(name) > 50 {
		return errors.New("bucket name is too long")
	}

	if !bucketName.MatchString(name) {
		return errors.New("bucket name contains invalid characters, allowed are: a-z, 0-9, dash (-)")
	}

	return nil
}

// ParseConfig parses the string s and extracts the fds config. The two
// supported configuration formats are fds://host/bucketname/prefix and
// fds:host/bucketname/prefix. The host can also be a valid fds region
// name. If no prefix is given the prefix "restic" will be used.
func ParseConfig(s string) (interface{}, error) {
	switch {
	case strings.HasPrefix(s, "fds://"):
		s = s[6:]
	case strings.HasPrefix(s, "fds:"):
		s = s[4:]
	default:
		return nil, errors.New("fds: invalid format")
	}
	// use the first entry of the path as the endpoint and the
	// remainder as bucket name and prefix
	path := strings.SplitN(s, "/", 3)
	return createConfig(path[0], path[1:])
}

func createConfig(endpoint string, p []string) (interface{}, error) {
	if len(p) < 1 {
		return nil, errors.New("fds: invalid format, host/region or bucket name not found")
	}

	var prefix string
	if len(p) > 1 && p[1] != "" {
		prefix = path.Clean(p[1])
	}

	cfg := NewConfig()
	cfg.FdsEndpoint = endpoint
	cfg.Bucket = p[0]
	cfg.Prefix = prefix

	if err := checkBucketName(cfg.Bucket); err != nil {
		return nil, err
	}

	return cfg, nil
}
