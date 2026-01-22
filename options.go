package workers

import (
	"crypto/tls"
	"errors"
	"log"
	"os"
	"strings"
	"time"

	"github.com/digitalocean/go-workers2/storage"
	"github.com/redis/rueidis"
	"github.com/redis/rueidis/rueidiscompat"
)

const (
	defaultHeartbeatInterval = 5 * time.Second

	defaultHeartbeatTTL = 60 * time.Second
)

// Options contains the set of configuration options for a manager and/or producer
type Options struct {
	ProcessID    string
	Namespace    string
	PollInterval time.Duration
	Database     int
	Password     string
	PipelineMultiplex     int
	IdleTimeout time.Duration

	// Provide one of ServerAddr or (SentinelAddrs + RedisMasterName)
	ServerAddr      string
	SentinelAddrs   string
	RedisMasterName string
	RedisTLSConfig  *tls.Config

	// Optional display name used when displaying manager stats
	ManagerDisplayName   string
	ManagerStartInactive bool

	// Define Heartbeat to enable heartbeat
	Heartbeat *HeartbeatOptions

	// Log
	Logger *log.Logger

	client rueidiscompat.Cmdable
	store  storage.Store
}

func (o *Options) Client() rueidiscompat.Cmdable {
	return o.client
}

type HeartbeatOptions struct {
	// Optional heartbeat interval config
	Interval time.Duration

	// redis eviction ttl config
	HeartbeatTTL time.Duration

	PrioritizedManager *PrioritizedManagerOptions
}

type PrioritizedManagerOptions struct {
	ManagerPriority     int
	TotalActiveManagers int
}

func processOptions(options Options) (Options, error) {
	options, err := validateGeneralOptions(options)
	if err != nil {
		return Options{}, err
	}

	//redis options
	if options.PipelineMultiplex == 0 {
		options.PipelineMultiplex = 1
	}
	if options.PipelineMultiplex >= rueidis.MaxPipelineMultiplex {
		options.PipelineMultiplex = rueidis.MaxPipelineMultiplex
	}

	if options.ServerAddr != "" {
		client, err := rueidis.NewClient(rueidis.ClientOption{
			Password: options.Password,
			SelectDB: options.Database,
			
			InitAddress: []string{options.ServerAddr},
			TLSConfig: options.RedisTLSConfig,

			BlockingPoolCleanup: options.IdleTimeout,
			PipelineMultiplex: options.PipelineMultiplex,
		})
		if err != nil {
			return Options{}, err
		}

		options.client = rueidiscompat.NewAdapter(client)
	} else if options.SentinelAddrs != "" {
		if options.RedisMasterName == "" {
			return Options{}, errors.New("Sentinel configuration requires a master name")
		}

		client, err := rueidis.NewClient(rueidis.ClientOption{
			SelectDB: options.Database,
			
			InitAddress: strings.Split(options.SentinelAddrs, ","),
			Sentinel: rueidis.SentinelOption{
				MasterSet: options.RedisMasterName,
				Password: options.Password,
				TLSConfig: options.RedisTLSConfig,
			},

			BlockingPoolCleanup: options.IdleTimeout,
			PipelineMultiplex: options.PipelineMultiplex,
		})
		if err != nil {
			return Options{}, err
		}

		options.client = rueidiscompat.NewAdapter(client)
	} else {
		return Options{}, errors.New("Options requires either the Server or Sentinels option")
	}

	if options.Logger == nil {
		options.Logger = log.New(os.Stdout, "go-workers2: ", log.Ldate|log.Lmicroseconds)
	}

	redisStore := storage.NewRedisStore(options.Namespace, options.client, options.Logger)
	options.store = redisStore

	if options.Heartbeat != nil {
		if options.Heartbeat.Interval <= 0 {
			options.Heartbeat.Interval = defaultHeartbeatInterval
		}
		if options.Heartbeat.HeartbeatTTL <= 0 {
			options.Heartbeat.HeartbeatTTL = defaultHeartbeatTTL
		}
	}

	return options, nil
}

func processOptionsWithRedisClient(options Options, client rueidiscompat.Cmdable) (Options, error) {
	options, err := validateGeneralOptions(options)
	if err != nil {
		return Options{}, err
	}

	if client == nil {
		return Options{}, errors.New("redis client is nil; Redis client is not configured")
	}

	options.client = client

	if options.Logger == nil {
		options.Logger = log.New(os.Stdout, "go-workers2: ", log.Ldate|log.Lmicroseconds)
	}

	redisStore := storage.NewRedisStore(options.Namespace, options.client, options.Logger)
	options.store = redisStore

	return options, nil
}

func validateGeneralOptions(options Options) (Options, error) {
	if options.ProcessID == "" {
		return Options{}, errors.New("options requires a ProcessID, which uniquely identifies this instance")
	}

	if options.Namespace != "" {
		options.Namespace += ":"
	}

	if options.PollInterval <= 0 {
		options.PollInterval = 15 * time.Second
	}

	if options.Heartbeat != nil &&
		options.Heartbeat.Interval >= options.Heartbeat.HeartbeatTTL {
		return Options{}, errors.New("invalid heartbeat configuration, heartbeat interval longer than or equal to heartbeat tll")
	}

	return options, nil
}
