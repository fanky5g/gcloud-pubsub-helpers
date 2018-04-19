package pubsub

import (
	"errors"
	"fmt"

	"cloud.google.com/go/pubsub"
	"golang.org/x/net/context"
)

// ListenOnSubscription starts a listener that listens on a particular action
func ListenOnSubscription(ctx context.Context, client *pubsub.Client, subscriptionID string, action Action, actionID string) error {
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	subscription := client.Subscription(subscriptionID)
	exists, err := subscription.Exists(ctx)
	if err != nil {
		return err
	}

	if !exists {
		return errors.New("Subscription does not exist")
	}

	subscription.ReceiveSettings.MaxOutstandingMessages = 50
	subscription.ReceiveSettings.MaxOutstandingBytes = 10e6

	err = subscription.Receive(ctx, func(ctx context.Context, m *pubsub.Message) {
		ctx, cancel := context.WithCancel(ctx)

		defer cancel()
		// run action
		if action != nil {
			done := make(chan struct{})
			cerror := make(chan error)
			go Run(ctx, action, actionID, m, done, cerror)

			select {
			case <-done:
				m.Ack()
				break
			case actionError := <-cerror:
				fmt.Printf("action errored: %v", actionError)
				m.Nack()
			}
		}
	})

	if err != context.Canceled {
		return err
	}

	return nil
}

// ListenOnceOnSubscription similar to ListenOnSubscription but closes after receiving once from subscription
func ListenOnceOnSubscription(ctx context.Context, client *pubsub.Client, subscriptionID string, action Action, actionID string) error {
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	subscription := client.Subscription(subscriptionID)
	exists, err := subscription.Exists(ctx)
	if err != nil {
		return err
	}

	if !exists {
		return errors.New("Subscription does not exist")
	}

	subscription.ReceiveSettings.MaxOutstandingMessages = 50
	subscription.ReceiveSettings.MaxOutstandingBytes = 10e6

	exit := make(chan bool, 1)
	go subscription.Receive(ctx, func(ctx context.Context, m *pubsub.Message) {
		_, cancel := context.WithCancel(ctx)

		defer cancel()
		defer func() {
			close(exit)
		}()

		// run action
		done := make(chan struct{})
		cerror := make(chan error)
		go Run(ctx, action, actionID, m, done, cerror)

		select {
		case <-done:
			m.Ack()
			break
		case actionError := <-cerror:
			fmt.Printf("action errored: %v", actionError)
			m.Nack()
			break
		}
	})

	<-exit
	return nil
}
