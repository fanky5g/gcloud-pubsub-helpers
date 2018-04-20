package pubsub

import (
	"errors"
	"fmt"
	"time"

	"cloud.google.com/go/pubsub"
	"golang.org/x/net/context"
)

/*
  google cloud pub/sub helpers
*/

// gcloud beta compute instance-groups managed set-autoscaling \
//     mediap-igr \
//     --zone=us-central1-b \
//     --max-num-replicas=100 \
//     --min-num-replicas=0 \
//     --update-stackdriver-metric=pubsub.googleapis.com/subscription/num_undelivered_messages \
//     --stackdriver-metric-filter="resource.type = pubsub_subscription AND resource.label.subscription_id = projects/silverbird-192810/subscriptions/mediap-sub	" \
//     --stackdriver-metric-single-instance-assignment=15

var (
	// PubsubClient is a wrapper for google cloud pubsub functions
	PubsubClient *pubsub.Client
)

// https://www.googleapis.com/compute/v1/projects/silverbird-192810/zones/us-central1-b/instanceGroupManagers/mediap-igr

// GetClient creates and returns google cloud pubsub client
func GetClient(projectID string) (*pubsub.Client, error) {
	ctx := context.Background()
	client, err := pubsub.NewClient(ctx, projectID)
	if err != nil {
		return nil, err
	}

	return client, nil
}

// Topic creates a pubsub topic
func Topic(client *pubsub.Client, topicID string) (*pubsub.Topic, error) {
	ctx := context.Background()
	topic := client.Topic(topicID)

	exists, err := client.Topic(topicID).Exists(ctx)
	if err != nil {
		return nil, err
	}

	if !exists {
		t, err := client.CreateTopic(ctx, topicID)
		if err != nil {
			return nil, err
		}
		topic = t
	}

	return topic, nil
}

// DeleteTopic deletes a pubsub topic
func DeleteTopic(client *pubsub.Client, topicID string) error {
	topic := client.Topic(topicID)
	ctx := context.Background()

	exists, err := topic.Exists(ctx)
	if err != nil {
		return err
	}

	if !exists {
		return errors.New("Topic does not exist")
	}

	return topic.Delete(ctx)
}

// CreateSubscription creates a pubsub subscription
func CreateSubscription(client *pubsub.Client, topicID string, subscriptionID string) (*pubsub.Subscription, error) {
	ctx := context.Background()
	topic := client.Topic(topicID)

	sub := client.Subscription(subscriptionID)
	exists, err := sub.Exists(ctx)
	if err != nil {
		return nil, err
	}

	if exists {
		return sub, nil
	}

	return client.CreateSubscription(ctx, subscriptionID, pubsub.SubscriptionConfig{
		Topic:       topic,
		AckDeadline: 300 * time.Second,
	})
}

// DeleteSubscription deletes a pubsub subscription
func DeleteSubscription(client *pubsub.Client, subscriptionID string) error {
	ctx := context.Background()
	subscription := client.Subscription(subscriptionID)

	exists, err := subscription.Exists(ctx)
	if err != nil {
		return err
	}

	if !exists {
		return errors.New("Subscription does not exist")
	}

	return subscription.Delete(ctx)
}

// Run runs actions inputing its payloads
func Run(ctx context.Context, action Action, actionID string, m *pubsub.Message, done chan struct{}, errored chan error) {
	cancelablectx, cancel := context.WithCancel(ctx)
	defer cancel()

	cres := make(chan ActionResponse, 1)
	cerror := make(chan error, 1)

	go func(ac Action, id string) {
		response := <-cres
		err := ac.OnSuccess(context.Background(), id, response)
		if err != nil {
			ac.OnError(context.Background(), id, err)
			errored <- err
			close(done)
		} else {
			done <- struct{}{}
		}
	}(action, actionID)

	go func(ac Action, id string) {
		err := <-cerror
		ac.OnError(context.Background(), id, err)
		errored <- err
		close(done)
	}(action, actionID)

	go func() {
		<-ctx.Done()
		fmt.Println("action cancelled")
		done <- struct{}{}
	}()

	go action.Execute(cancelablectx, actionID, m.Data, cres, cerror)
	<-done
}
