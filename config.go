package daomq

import (
	"os"
	"strconv"
	"time"
)

type DBConfig struct {
	DSN             string        `json:"DSN"`
	MaxOpenConns    int           `json:"MaxOpenConns"`
	MaxIdleConns    int           `json:"MaxIdleConns"`
	ConnMaxLifetime time.Duration `json:"ConnMaxLifetime"`
}

type MQConfig struct {
	QueueTable    string `json:"QueueTable"`
	MsgQueueTable string `json:"MsgQueueTable"`
}

type BrokerConfig struct {
	// Broker监听信号通道
	isExitC chan os.Signal
}

type Config struct {
	*DBConfig
	*MQConfig
	*BrokerConfig
}

var DefaultDBOption = DBConfig{
	DSN:             "user:password@tcp(dbserver)/database",
	MaxOpenConns:    10,
	MaxIdleConns:    5,
	ConnMaxLifetime: 3600,
}

var DefaultMQOption = MQConfig{
	QueueTable:    "daomq_queue",
	MsgQueueTable: "daomq_message",
}

type Option func(*Config)

// 初始化配置文件
func NewConfig() *Config {
	c := &Config{
		DBConfig:     &DefaultDBOption,
		MQConfig:     &DefaultMQOption,
		BrokerConfig: &BrokerConfig{},
	}
	c.getConfigFromEnv()
	return c
}

// 从环境变量获取配置
func (c *Config) getConfigFromEnv() {
	if res, ok := os.LookupEnv("DSN"); ok {
		c.DSN = res
	}
	if res, ok := os.LookupEnv("MaxOpenConns"); ok {
		if resInt, err := strconv.Atoi(res); err == nil {
			c.MaxOpenConns = resInt
		}
	}
	if res, ok := os.LookupEnv("MaxIdleConns"); ok {
		if resInt, err := strconv.Atoi(res); err == nil {
			c.MaxIdleConns = resInt
		}
	}
	if res, ok := os.LookupEnv("ConnMaxLifetime"); ok {
		if resInt, err := strconv.Atoi(res); err == nil {
			c.ConnMaxLifetime = time.Duration(resInt * int(time.Second))
		}
	}
	if res, ok := os.LookupEnv("QueueTable"); ok {
		c.QueueTable = res
	}
	if res, ok := os.LookupEnv("MsgQueueTable"); ok {
		c.MsgQueueTable = res
	}
}

// 设置Broker监听信号通道，用于控制启动消费模式产生的消费者协程和broker loop协程的优雅退出
func SetExitCOption(ch chan os.Signal) Option {
	return func(c *Config) {
		c.isExitC = ch
	}
}

// 设置DB配置的DSN
func SetDBDSNOption(DSN string) Option {
	return func(c *Config) {
		c.DSN = DSN
	}
}
