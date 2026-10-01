// Package faultproxy 是测试用的 TCP 故障注入代理，可模拟网络黑洞与连接重置。
package faultproxy

import (
	"net"
	"sync"
)

// Proxy 把本地端口的连接转发到 target。
//
// 黑洞模式下暂停双向转发但保持 socket：读到的数据先扣住，恢复后原样写出，
// 与真实网络丢包后 TCP 重传恢复的效果一致，字节流不会损坏。
type Proxy struct {
	ln     net.Listener
	target string

	mu        sync.Mutex
	cond      *sync.Cond
	blackhole bool
	closed    bool
	conns     map[net.Conn]struct{}
	accepted  int
}

func New(target string) (*Proxy, error) {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		return nil, err
	}
	p := &Proxy{ln: ln, target: target, conns: make(map[net.Conn]struct{})}
	p.cond = sync.NewCond(&p.mu)
	go p.accept()
	return p, nil
}

func (p *Proxy) Addr() string { return p.ln.Addr().String() }

// Accepted 返回累计接受的客户端连接数。
func (p *Proxy) Accepted() int {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.accepted
}

func (p *Proxy) SetBlackhole(on bool) {
	p.mu.Lock()
	p.blackhole = on
	p.mu.Unlock()
	p.cond.Broadcast()
}

// ResetAll 立即关闭所有已建立的连接（客户端与上游两侧）。
func (p *Proxy) ResetAll() {
	p.mu.Lock()
	conns := make([]net.Conn, 0, len(p.conns))
	for c := range p.conns {
		conns = append(conns, c)
	}
	p.mu.Unlock()
	for _, c := range conns {
		_ = c.Close()
	}
}

func (p *Proxy) Close() {
	p.mu.Lock()
	p.closed = true
	p.blackhole = false
	p.mu.Unlock()
	p.cond.Broadcast()
	_ = p.ln.Close()
	p.ResetAll()
}

func (p *Proxy) accept() {
	for {
		c, err := p.ln.Accept()
		if err != nil {
			return
		}
		go p.handle(c)
	}
}

func (p *Proxy) handle(client net.Conn) {
	upstream, err := net.Dial("tcp", p.target)
	if err != nil {
		_ = client.Close()
		return
	}
	p.mu.Lock()
	if p.closed {
		p.mu.Unlock()
		_ = client.Close()
		_ = upstream.Close()
		return
	}
	p.accepted++
	p.conns[client] = struct{}{}
	p.conns[upstream] = struct{}{}
	p.mu.Unlock()

	var wg sync.WaitGroup
	wg.Add(2)
	go func() { defer wg.Done(); p.pipe(upstream, client) }()
	go func() { defer wg.Done(); p.pipe(client, upstream) }()
	wg.Wait()

	p.mu.Lock()
	delete(p.conns, client)
	delete(p.conns, upstream)
	p.mu.Unlock()
}

func (p *Proxy) pipe(dst, src net.Conn) {
	defer func() {
		_ = dst.Close()
		_ = src.Close()
	}()
	buf := make([]byte, 32*1024)
	for {
		if !p.waitOpen() {
			return
		}
		n, err := src.Read(buf)
		if n > 0 {
			if !p.waitOpen() {
				return
			}
			if _, werr := dst.Write(buf[:n]); werr != nil {
				return
			}
		}
		if err != nil {
			return
		}
	}
}

func (p *Proxy) waitOpen() bool {
	p.mu.Lock()
	defer p.mu.Unlock()
	for p.blackhole && !p.closed {
		p.cond.Wait()
	}
	return !p.closed
}
