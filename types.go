package pubsub

import (
	"context"
)

type Action interface {
	Execute(context.Context, string, []byte, chan ActionResponse, chan error)
	OnSuccess(context.Context, string, interface{}) error
	OnError(context.Context, string, error)
}

type ActionResponse interface {
	GetVal() interface{}
}
