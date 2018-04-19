package pubsub

import (
	"context"
)

type Action interface {
	Execute(context.Context, string, []byte, chan struct{}, chan error) error
	OnSuccess(context.Context, string, interface{}) error
	OnError(context.Context, string, error)
}
