package broker

import (
	"context"
	"errors"
	"fmt"
	"log"
	"net"
	"os"
	"sync"
	"time"

	"github.com/streadway/amqp"

	"github.com/afret0/rabbitmq/internal/backoff"
)

const (
	dialTimeout       = 10 * time.Second
	heartbeatInterval = 10 * time.Second

	// rpcTimeout 是单次 AMQP RPC（开 channel、声明、发布写入）的上限。
	// streadway 的 RPC 没有超时：连接被 broker 阻塞（内存/磁盘告警）或网络黑洞时会永久挂起。
	// 超过该时间即判定连接不可用，强制关闭底层 socket，让所有挂起调用立即返回并触发重连。
	rpcTimeout = 10 * time.Second

	connCloseTimeout    = 3 * time.Second
	reconnectMin        = 500 * time.Millisecond
	reconnectMax        = 30 * time.Second
	stableConnThreshold = 30 * time.Second
)

var (
	// ErrShutdown 表示 broker 已被 Shutdown / Close，不再接受新的消费或发布。
	ErrShutdown = errors.New("rabbitmq broker is shut down")

	// ErrBlocked 表示连接被 broker 流控阻塞（如内存/磁盘告警），在超时时间内未解除。
	// 注意：阻塞期间已写出的消息在告警解除后仍可能被投递。
	ErrBlocked = errors.New("amqp connection blocked by broker")

	errNotConnected = errors.New("amqp connection is not ready")
)

var hostname = func() string {
	h, err := os.Hostname()
	if err != nil || h == "" {
		return "unknown"
	}
	return h
}()

// connManager 维护一条可自动重连的 AMQP 连接。
//
// 每次建立新连接 gen 自增，强制关闭只作用于指定 gen，避免误杀已重连的新连接。
type connManager struct {
	url  string
	name string

	startOnce sync.Once

	mu           sync.Mutex
	conn         *amqp.Connection
	raw          net.Conn
	closeCh      chan *amqp.Error
	gen          uint64
	installedAt  time.Time
	ready        chan struct{} // 有可用连接时为关闭状态
	blocked      bool
	blockedWhy   string
	blockedSig   chan struct{} // 被阻塞时为关闭状态
	unblockedSig chan struct{} // 未被阻塞时为关闭状态
	closed       bool
	done         chan struct{}
}

func newConnManager(url, name string) *connManager {
	m := &connManager{
		url:          url,
		name:         name,
		ready:        make(chan struct{}),
		blockedSig:   make(chan struct{}),
		unblockedSig: make(chan struct{}),
		done:         make(chan struct{}),
	}
	close(m.unblockedSig)
	return m
}

// connect 同步建连一次（保持 NewAmqpBroker 启动即失败的语义），成功后启动重连循环。
func (m *connManager) connect() error {
	c, raw, err := m.dial()
	if err != nil {
		return err
	}
	if !m.install(c, raw) {
		return ErrShutdown
	}
	m.start()
	return nil
}

func (m *connManager) start() {
	m.startOnce.Do(func() { go m.run() })
}

func (m *connManager) dial() (*amqp.Connection, net.Conn, error) {
	// 关闭后不再建连；正在进行的 TCP 建连随关闭取消
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go func() {
		select {
		case <-m.done:
			cancel()
		case <-ctx.Done():
		}
	}()
	if ctx.Err() != nil || m.isClosed() {
		return nil, nil, ErrShutdown
	}

	var raw net.Conn
	cfg := amqp.Config{
		Heartbeat: heartbeatInterval,
		Locale:    "en_US",
		Properties: amqp.Table{
			"product":         "https://github.com/streadway/amqp",
			"version":         "β",
			"connection_name": fmt.Sprintf("%s/%s", hostname, m.name),
		},
		Dial: func(network, addr string) (net.Conn, error) {
			d := net.Dialer{Timeout: dialTimeout}
			c, err := d.DialContext(ctx, network, addr)
			if err != nil {
				return nil, err
			}
			// 握手阶段的超时，握手完成后 streadway 会清除，之后由心跳负责探活
			if err := c.SetDeadline(time.Now().Add(dialTimeout)); err != nil {
				_ = c.Close()
				return nil, err
			}
			raw = c
			return c, nil
		},
	}
	c, err := amqp.DialConfig(m.url, cfg)
	if err != nil {
		if raw != nil {
			_ = raw.Close()
		}
		return nil, nil, err
	}
	return c, raw, nil
}

func (m *connManager) install(c *amqp.Connection, raw net.Conn) bool {
	closeCh := c.NotifyClose(make(chan *amqp.Error, 1))
	blockCh := c.NotifyBlocked(make(chan amqp.Blocking, 4))

	m.mu.Lock()
	if m.closed {
		m.mu.Unlock()
		_ = raw.Close()
		return false
	}
	m.gen++
	gen := m.gen
	m.conn, m.raw, m.closeCh = c, raw, closeCh
	m.installedAt = time.Now()
	m.setBlockedLocked(false, "")
	close(m.ready)
	m.mu.Unlock()

	go m.watchBlocked(gen, blockCh)
	return true
}

func (m *connManager) uninstall(gen uint64) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.gen != gen || m.conn == nil {
		return
	}
	m.conn, m.raw, m.closeCh = nil, nil, nil
	m.ready = make(chan struct{})
	m.setBlockedLocked(false, "")
}

func (m *connManager) setBlockedLocked(blocked bool, why string) {
	if blocked == m.blocked {
		return
	}
	m.blocked, m.blockedWhy = blocked, why
	if blocked {
		close(m.blockedSig)
		m.unblockedSig = make(chan struct{})
	} else {
		close(m.unblockedSig)
		m.blockedSig = make(chan struct{})
	}
}

// watchBlocked 必须持续消费：streadway 在读循环里同步投递阻塞通知，不读会卡死整条连接。
func (m *connManager) watchBlocked(gen uint64, ch <-chan amqp.Blocking) {
	for b := range ch {
		m.mu.Lock()
		if m.gen == gen && m.conn != nil {
			m.setBlockedLocked(b.Active, b.Reason)
		}
		m.mu.Unlock()
		if b.Active {
			log.Printf("rabbitmq: %s connection blocked by broker: %s", m.name, b.Reason)
		} else {
			log.Printf("rabbitmq: %s connection unblocked", m.name)
		}
	}
}

func (m *connManager) run() {
	bo := backoff.New(reconnectMin, reconnectMax)
	// 只有懒加载连接的第一次建连不等待；之后每次拨号前都带抖动退避，拨号持续失败也不会空转
	first := true
	for {
		if m.isClosed() {
			return
		}
		m.mu.Lock()
		closeCh, gen, installedAt := m.closeCh, m.gen, m.installedAt
		m.mu.Unlock()

		if closeCh != nil {
			select {
			case err := <-closeCh:
				log.Printf("rabbitmq: %s connection closed: %v", m.name, err)
				m.uninstall(gen)
				if time.Since(installedAt) > stableConnThreshold {
					bo.Reset()
				}
			case <-m.done:
				return
			}
			first = false
		}

		if !first && !m.sleep(bo.Next()) {
			return
		}
		first = false

		c, raw, err := m.dial()
		if err != nil {
			if errors.Is(err, ErrShutdown) {
				return
			}
			log.Printf("rabbitmq: %s connection dial failed: %v", m.name, err)
			continue
		}
		if !m.install(c, raw) {
			return
		}
		log.Printf("rabbitmq: %s connection established", m.name)
	}
}

func (m *connManager) sleep(d time.Duration) bool {
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-t.C:
		return true
	case <-m.done:
		return false
	}
}

// get 返回当前可用连接；断线重连期间等待，直到 ctx 结束或 broker 关闭。
func (m *connManager) get(ctx context.Context) (*amqp.Connection, uint64, error) {
	if m.isClosed() {
		return nil, 0, ErrShutdown
	}
	m.start()
	for {
		m.mu.Lock()
		if m.closed {
			m.mu.Unlock()
			return nil, 0, ErrShutdown
		}
		c, gen, ready := m.conn, m.gen, m.ready
		m.mu.Unlock()

		if c != nil {
			if !c.IsClosed() {
				return c, gen, nil
			}
			// 连接已断但重连循环还没处理，先摘除，避免对已关闭的 ready 空转
			m.uninstall(gen)
			continue
		}

		select {
		case <-ready:
		case <-ctx.Done():
			return nil, 0, ctx.Err()
		case <-m.done:
			return nil, 0, ErrShutdown
		}
	}
}

// waitUnblocked 在连接被 broker 阻塞时等待解除。
func (m *connManager) waitUnblocked(ctx context.Context, gen uint64) error {
	m.mu.Lock()
	if m.gen != gen || !m.blocked {
		m.mu.Unlock()
		return nil
	}
	sig, why := m.unblockedSig, m.blockedWhy
	m.mu.Unlock()

	select {
	case <-sig:
		return nil
	case <-ctx.Done():
		if errors.Is(ctx.Err(), context.DeadlineExceeded) {
			return fmt.Errorf("%w: %s", ErrBlocked, why)
		}
		return ctx.Err()
	case <-m.done:
		return ErrShutdown
	}
}

// blockedChan 返回第 gen 代连接被阻塞时关闭的 channel；连接已更替时返回 nil（永不触发）。
func (m *connManager) blockedChan(gen uint64) <-chan struct{} {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.gen != gen {
		return nil
	}
	return m.blockedSig
}

func (m *connManager) blockedReason(gen uint64) (bool, string) {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.gen == gen && m.blocked, m.blockedWhy
}

// hardClose 直接关闭底层 socket。streadway 的读循环随即报错并关闭连接，
// 所有挂起的 RPC / 写入都会返回，随后由重连循环重建连接。
func (m *connManager) hardClose(gen uint64, reason string) {
	m.mu.Lock()
	if m.gen != gen || m.raw == nil {
		m.mu.Unlock()
		return
	}
	raw := m.raw
	m.mu.Unlock()
	log.Printf("rabbitmq: %s connection force closed: %s", m.name, reason)
	_ = raw.Close()
}

// watchdog 在 d 之后强制关闭第 gen 代连接，调用方完成操作后必须 Stop。
func (m *connManager) watchdog(gen uint64, d time.Duration, what string) *time.Timer {
	return time.AfterFunc(d, func() {
		m.hardClose(gen, fmt.Sprintf("%s did not finish within %v", what, d))
	})
}

// withWatchdog 同步执行 fn，超时则强制关闭连接，保证 fn 一定在 d 左右返回。
func (m *connManager) withWatchdog(gen uint64, d time.Duration, what string, fn func() error) error {
	t := m.watchdog(gen, d, what)
	defer t.Stop()
	return fn()
}

func (m *connManager) isClosed() bool {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.closed
}

func (m *connManager) healthy() bool {
	m.mu.Lock()
	defer m.mu.Unlock()
	return !m.closed && m.conn != nil && !m.conn.IsClosed()
}

// close 停止重连并关闭连接：先优雅关闭，超时则强制关闭 socket。
func (m *connManager) close() {
	m.mu.Lock()
	if m.closed {
		m.mu.Unlock()
		return
	}
	m.closed = true
	close(m.done)
	c, raw := m.conn, m.raw
	m.mu.Unlock()

	if c == nil {
		return
	}
	done := make(chan struct{})
	go func() {
		_ = c.Close()
		close(done)
	}()
	t := time.NewTimer(connCloseTimeout)
	defer t.Stop()
	select {
	case <-done:
	case <-t.C:
		_ = raw.Close()
	}
}
