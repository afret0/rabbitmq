package rabbitmq

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/afret0/wheel/log"
	"github.com/afret0/wheel/tool"
	"github.com/sirupsen/logrus"
	"golang.org/x/sync/errgroup"
)

type Order struct {
	OrderID int64 `json:"orderId"`
	Amount  int64 `json:"amount"`
}

var opt = &ExchangeOption{
	Name: "test-topic-exchange",
	Type: "topic",
}
var brokerUrl = "amqp://MjpyYWJiaXRtcS1jbi1xem00ZHNrN2IwNjpMVEFJNXRRSHF0djFXYkN4NXB4YXk2TEw=:MEVGOEFBODlCRDBCN0NBNjJEMjU5NTI1REYxOTU1MTdBODk1MTFFRToxNzU3MDQzNjczOTM0@rabbitmq-cn-qzm4dsk7b06-cn-hangzhou-amqp-46-net.mq.amqp.aliyuncs.com"

var conOpt = &ConsumerOptions{
	ExchangeOpt: opt,
	BrokerURL:   brokerUrl,
}

var proOpt = &ProducerOptions{
	ExchangeOpt: opt,
	BrokerURL:   brokerUrl,
}

var consumer = NewConsumer(conOpt)
var producer = NewProducer(proOpt)

func F(ctx context.Context, o *Order) error {
	lg := log.CtxLogger(ctx).WithFields(logrus.Fields{})

	lg.Infof("F: %s", tool.MarshalWithoutErr(o))
	return nil
}

func Test_NewJob(t *testing.T) {
	eg := errgroup.Group{}

	k := "test-new-job"

	eg.Go(func() error {

		for now := range time.Tick(time.Second) {
			producer.Publish(tool.NewCtxBK(), k, &Message[*Order]{
				MsgId: fmt.Sprintf("%d", now.Unix()),
				Data: &Order{
					OrderID: now.Unix(),
					Amount:  now.Unix() * 100,
				},
			})
		}
		return nil
	})

	eg.Go(func() error {
		consumer.LaunchJob(k, k, NewJob[*Order](F))
		return nil
	})

	eg.Wait()
}
