package pubsub

import (
	"cloud.google.com/go/pubsub"
	"golang.org/x/net/context"
)

// Publish publishes a message to a topic
func Publish(client *pubsub.Client, topicID string, attrs map[string]string, payload []byte) *pubsub.PublishResult {
	ctx := context.Background()

	topic := client.Topic(topicID)
	defer topic.Stop()

	return topic.Publish(ctx, &pubsub.Message{
		Data:       payload,
		Attributes: attrs,
	})
}
