package pubsub

import (
	"log"
	"os"
	"testing"

	"cloud.google.com/go/pubsub"
	"github.com/stretchr/testify/assert"
	"golang.org/x/net/context"
)

var (
	testTopic  *pubsub.Topic
	testClient *pubsub.Client
)

func TestMain(m *testing.M) {
	client, err := GetClient("test_client_id")
	if err != nil {
		log.Fatal(err)
	}

	testClient = client

	code := m.Run()
	os.Exit(code)
}

func TestCreateTopic(t *testing.T) {
	topic, err := Topic(testClient, "cfg.PubSubVideoPipelineTopic")
	assert.NoError(t, err)
	assert.NotNil(t, topic)
}

func TestCreateSubscription(t *testing.T) {
	sub, err := CreateSubscription(testClient, "cfg.PubSubVideoPipelineTopic", "cfg.PubSubVideoPipelineSub")
	assert.NoError(t, err)
	assert.NotNil(t, sub)
}

type a struct{}

func (action a) Execute(ctx context.Context, id string, data []byte, response chan struct{}, errChan chan error) error {
	response <- struct{}{}
	return nil
}

func (action a) OnError(ctx context.Context, id string, err error) {}
func (action a) OnSuccess(ctx context.Context, id string, response interface{}) error {
	return nil
}

func TestListenOnSubscription(t *testing.T) {
	ctx := context.Background()

	err := ListenOnSubscription(ctx, testClient, "", a{}, "")
	assert.NoError(t, err)
}
