package rabbitmq

import (
	"context"
	"testing"
	"time"
)

func Test_Rabbitmq(t *testing.T) {
	topic := "test-topic"
	group := "test-group"
	groupV1 := "test-group-v1"
	groupV2 := "test-group-v2"

	opt := &ExchangeOption{
		Name: "test-topic-exchange",
		Type: "topic",
	}
	brokerUrl := "amqp://MjpyYWJiaXRtcS1jbi1xem00ZHNrN2IwNjpMVEFJNXRRSHF0djFXYkN4NXB4YXk2TEw=:MEVGOEFBODlCRDBCN0NBNjJEMjU5NTI1REYxOTU1MTdBODk1MTFFRToxNzU3MDQzNjczOTM0@rabbitmq-cn-qzm4dsk7b06-cn-hangzhou-amqp-46-net.mq.amqp.aliyuncs.com"

	conOpt := &ConsumerOptions{
		ExchangeOpt: opt,
		BrokerURL:   brokerUrl,
	}

	proOpt := &ProducerOptions{
		ExchangeOpt: opt,
		BrokerURL:   brokerUrl,
	}

	consumer := NewConsumer(conOpt)
	producer := NewProducer(proOpt)

	go func() {
		for now := range time.Tick(3 * time.Second) {
			err := producer.Publish(context.Background(), topic, map[string]string{"now": now.String()})
			if err != nil {
				t.Error(err)
			}

			t.Logf("       ")
			t.Logf("       ")
			t.Logf("producer publish msg: %s", now.String())
		}

	}()

	go func() {

		job := func(body []byte) error {
			t.Logf("group: %s, consumer receive msg: %s", group, body)
			return nil
		}

		consumer.LaunchTopicJob(group, topic, job)
	}()

	go func() {
		job := func(body []byte) error {
			t.Logf("group: %s, consumerV1 receive msg: %s", group, body)
			return nil
		}

		consumer.LaunchTopicJob(group, topic, job)
	}()

	go func() {
		job := func(body []byte) error {
			t.Logf("group: %s, consumerV1 receive msg: %s", groupV1, body)
			return nil
		}
		consumer.LaunchTopicJob(groupV1, topic, job)
	}()

	go func() {
		job := func(body []byte) error {
			t.Logf("group: %s, consumer receive msg: %s", groupV2, body)
			return nil
		}
		consumer.LaunchTopicJob(groupV2, topic, job)
	}()

	job := func(body []byte) error {
		t.Logf("group: %s, consumer receive msg: %s", groupV1, body)
		return nil
	}
	consumer.LaunchTopicJob(groupV1, topic, job)
}
