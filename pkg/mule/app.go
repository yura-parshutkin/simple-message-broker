package mule

import (
	"context"
)

type ConfigItem struct {
	QueueName string
	Size      int
	SubsSize  int
}

type Config []ConfigItem

type Server interface {
	Start() error
	Stop(ctx context.Context) error
}
