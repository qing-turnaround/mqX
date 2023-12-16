package redis

const (
	// DefaultIdleTimeoutSeconds 默认连接池超过 10 s 释放连接
	DefaultIdleTimeoutSeconds = 10
	// DefaultMaxActive 默认最大激活连接数
	DefaultMaxActive = 100
	// DefaultMaxIdle 默认最大空闲连接数
	DefaultMaxIdle = 20
)

type ClientOptions struct {
	maxIdleConns       int
	idleTimeoutSeconds int
	maxActiveConns     int
	wait               bool
	db                 int
	// 必填参数
	network  string
	address  string
	password string
}

type ClientOption func(c *ClientOptions)

func WithMaxIdle(maxIdle int) ClientOption {
	return func(c *ClientOptions) {
		c.maxIdleConns = maxIdle
	}
}

func WithIdleTimeoutSeconds(idleTimeoutSeconds int) ClientOption {
	return func(c *ClientOptions) {
		c.idleTimeoutSeconds = idleTimeoutSeconds
	}
}

func WithMaxActive(maxActive int) ClientOption {
	return func(c *ClientOptions) {
		c.maxActiveConns = maxActive
	}
}

func WithDB(db int) ClientOption {
	return func(c *ClientOptions) {
		c.db = db
	}
}

func repairClient(c *ClientOptions) {
	if c.maxIdleConns < 0 {
		c.maxIdleConns = DefaultMaxIdle
	}

	if c.idleTimeoutSeconds < 0 {
		c.idleTimeoutSeconds = DefaultIdleTimeoutSeconds
	}

	if c.maxActiveConns < 0 {
		c.maxActiveConns = DefaultMaxActive
	}
}
