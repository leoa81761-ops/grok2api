package inference

import (
	"net/http"
	"sync"
	"time"

	"github.com/gin-gonic/gin"
)

// streamKeepaliveInterval is how long the upstream may stay silent before a
// comment line is written to the downstream connection.
const streamKeepaliveInterval = 15 * time.Second

// streamKeepaliveComment is an SSE comment: parsers must ignore it, so it never
// reaches the message body.
const streamKeepaliveComment = ": keepalive\n\n"

// streamKeepalive keeps an idle Server-Sent-Events response alive while the
// upstream is thinking. Cloudflare drops an origin connection after ~120s with
// no bytes, and an intermediate client (new-api, an agent, a terminal) may also
// give up on an empty first byte, so long high-reasoning turns must send
// something during the silent window.
//
// Every write path takes the lock, so the response keeps a single writer and
// stop() can guarantee the heartbeat goroutine has exited before the handler
// returns.
type streamKeepalive struct {
	gin.ResponseWriter
	interval time.Duration
	mu       sync.Mutex
	once     sync.Once
	last     time.Time
	done     chan struct{}
	finished chan struct{}
}

func newStreamKeepalive(writer gin.ResponseWriter, interval time.Duration) *streamKeepalive {
	value := &streamKeepalive{
		ResponseWriter: writer,
		interval:       interval,
		last:           time.Now(),
		done:           make(chan struct{}),
		finished:       make(chan struct{}),
	}
	if interval > 0 {
		go value.loop()
	} else {
		close(value.finished)
	}
	return value
}

func (k *streamKeepalive) loop() {
	defer close(k.finished)
	ticker := time.NewTicker(k.interval)
	defer ticker.Stop()
	for {
		select {
		case <-k.done:
			return
		case now := <-ticker.C:
			k.mu.Lock()
			// Only fill a real gap: recent bytes mean the connection is alive.
			if now.Sub(k.last) >= k.interval {
				_, _ = k.ResponseWriter.WriteString(streamKeepaliveComment)
				k.ResponseWriter.Flush()
				k.last = time.Now()
			}
			k.mu.Unlock()
		}
	}
}

func (k *streamKeepalive) Write(payload []byte) (int, error) {
	k.mu.Lock()
	defer k.mu.Unlock()
	k.last = time.Now()
	return k.ResponseWriter.Write(payload)
}

func (k *streamKeepalive) WriteString(value string) (int, error) {
	k.mu.Lock()
	defer k.mu.Unlock()
	k.last = time.Now()
	return k.ResponseWriter.WriteString(value)
}

func (k *streamKeepalive) Flush() {
	k.mu.Lock()
	defer k.mu.Unlock()
	k.ResponseWriter.Flush()
}

// Unwrap lets http.ResponseController reach the underlying writer.
func (k *streamKeepalive) Unwrap() http.ResponseWriter { return k.ResponseWriter }

// stop ends the heartbeat and waits for the goroutine, so no write can happen
// after the handler returns. It is safe to call more than once.
func (k *streamKeepalive) stop() {
	k.once.Do(func() { close(k.done) })
	<-k.finished
}
