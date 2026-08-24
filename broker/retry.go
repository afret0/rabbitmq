package broker

// ackAction 描述一条消息在 handler 要求重试后应当如何回执。
type ackAction int

const (
	// actionAck 确认消息：重试已被投递到延迟队列，原消息可以安全丢弃。
	actionAck ackAction = iota
	// actionRequeue 重新入队：消息还需要被再次投递，绝不能丢。
	actionRequeue
	// actionReject 拒绝消息：重试次数已用尽，交给死信(未配置死信则丢弃)。
	actionReject
)

// retryAction 决定 handler 返回 Retry 后的回执方式。
//
// 之所以要区分这三种情况，是因为「调度重试失败」「未配置重试」「重试用尽」
// 都会让 retry() 返回 nil error，若一律 Ack 就会静默丢消息。
//
//   - scheduled: 已成功投递到延迟队列
//   - err:       调度过程本身出错
//   - retryConfigured: 队列是否配置了重试退避序列
func retryAction(scheduled bool, err error, retryConfigured bool) ackAction {
	switch {
	case err != nil:
		// 调度失败, 重新入队等待下次投递, 不能丢
		return actionRequeue
	case scheduled:
		return actionAck
	case !retryConfigured:
		// 未配置延迟重试: 退化为 requeue 立即重投, 而不是静默丢弃
		return actionRequeue
	default:
		// 配置了重试且次数已用尽: 终止重试
		return actionReject
	}
}
