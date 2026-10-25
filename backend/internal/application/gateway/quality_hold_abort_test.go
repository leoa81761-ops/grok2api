package gateway

import (
	"context"
	"errors"
	"io"
	"testing"
	"time"
)

// TestPeekQualityStreamAbortsHoldTimeoutWithoutOutput 验证形态 B 的修复：
// 开关打开时，持流到期且上游零输出必须立刻按空流失败返回（网关换号/快速 502），
// 而不是继续 Wait 让下游长时间零字节、最终被客户端以 client_stream_interrupted
// 掐断（线上实测 ≈62s、output=0）。
func TestPeekQualityStreamAbortsHoldTimeoutWithoutOutput(t *testing.T) {
	reader, writer := io.Pipe()
	defer writer.Close()
	cfg := QualityRetryRuntime{
		MinOutputTokens:          8,
		HoldTimeout:              20 * time.Millisecond,
		AbortHoldTimeoutNoOutput: true,
	}
	type outcome struct {
		err    error
		verdict QualityVerdict
	}
	done := make(chan outcome, 1)
	go func() {
		_, verdict, _, _, err := peekQualityStream(context.Background(), reader, qualityProtocolChat, cfg)
		done <- outcome{err: err, verdict: verdict}
	}()
	select {
	case got := <-done:
		if !errors.Is(got.err, errQualityEmptyStream) {
			t.Fatalf("err = %v, want errQualityEmptyStream", got.err)
		}
		if got.verdict != QualityWait {
			t.Fatalf("verdict = %s, want wait", got.verdict)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("peekQualityStream did not abort on hold timeout without output")
	}
}

// TestPeekQualityStreamHoldTimeoutKeepsWaitingByDefault 确认开关关闭时保持原有
// 语义：空流持流到期继续等待（不能 fail-open 交付空响应）。
func TestPeekQualityStreamHoldTimeoutKeepsWaitingByDefault(t *testing.T) {
	reader, writer := io.Pipe()
	defer writer.Close()
	cfg := QualityRetryRuntime{MinOutputTokens: 8, HoldTimeout: 20 * time.Millisecond}
	done := make(chan struct{})
	go func() {
		defer close(done)
		_, _, _, _, _ = peekQualityStream(context.Background(), reader, qualityProtocolChat, cfg)
	}()
	select {
	case <-done:
		t.Fatal("without the flag the hold must keep waiting")
	case <-time.After(120 * time.Millisecond):
	}
	_ = writer.Close()
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("peekQualityStream did not return after upstream close")
	}
}
