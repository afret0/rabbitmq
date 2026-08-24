package rabbitmq

import (
	"testing"
	"time"

	"github.com/afret0/wheel/tool"
)

func Test_Limit(t *testing.T) {
	opt := &ExchangeOption{
		Name: "test-v1-exchange",
		Type: "direct",
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

	ctx := tool.NewCtxBK()
	Q := "test-v1"

	remain := 0

	go func() {
		for now := range time.Tick(1 * time.Second) {
			producer.Publish(ctx, Q, map[string]string{"now": now.String()})
			t.Logf("+now: %s", now.String())
			remain++
		}
	}()

	consumer.LaunchJob(Q, Q, func(body []byte) error {
		t.Logf("-now: %s", string(body))
		remain--
		t.Logf("remain: %d", remain)
		return nil
	}, LimitEvery(2*time.Second, 1))

}
