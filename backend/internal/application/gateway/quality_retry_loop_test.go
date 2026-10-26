package gateway

import (
	"fmt"
	"strings"
	"testing"
)

func TestQualityIsDegenerateLoop(t *testing.T) {
	// 18.5k visible tokens (74k runes) with one 24-rune window repeated 900 times
	// is the shipped 32k repetition shape: window coverage ≈ 1.16x the visible text.
	if !qualityIsDegenerateLoop(QualityStreamSignals{VisibleTokens: 18500, RepeatMax: 900}) {
		t.Fatal("repeated window with no thinking must be withheld")
	}
	cases := []struct {
		name string
		sig  QualityStreamSignals
	}{
		{"below repeat threshold", QualityStreamSignals{VisibleTokens: 18500, RepeatMax: 40}},
		{"too little visible text", QualityStreamSignals{VisibleTokens: 64, RepeatMax: 400}},
		{"repeats too sparse in a long answer", QualityStreamSignals{VisibleTokens: 100000, RepeatMax: 60}},
		{"plaintext reasoning seen", QualityStreamSignals{VisibleTokens: 18500, RepeatMax: 900, HasReasoningDelta: true}},
		{"cipher thinking seen", QualityStreamSignals{VisibleTokens: 18500, RepeatMax: 900, HasThinking: true}},
		{"tool call present", QualityStreamSignals{VisibleTokens: 18500, RepeatMax: 900, SemanticOutput: true}},
	}
	for _, c := range cases {
		if qualityIsDegenerateLoop(c.sig) {
			t.Fatalf("%s: must not be withheld", c.name)
		}
	}
}

func TestDegenerateLoopWithholdsInClassifier(t *testing.T) {
	verdict, rule := classifyQualityHold(QualityStreamSignals{VisibleTokens: 18500, RepeatMax: 900, Terminal: true}, 8)
	if verdict != QualityWithhold || rule != qualityWithholdRuleDegenerateLoop {
		t.Fatalf("verdict=%v rule=%v", verdict, rule)
	}
}

// TestObserveQualityChunkDegenerateRepeat drives the real scanner with the
// shipped 32k repetition shape: one sentence repeated until the output cap,
// no reasoning delta, no tool call.
func TestObserveQualityChunkDegenerateRepeat(t *testing.T) {
	state := qualityScanState{protocol: qualityProtocolChat}
	sentence := "我把 Worker 运行器强制隔离，并把默认 dispatch 改成使用 runner。"
	ObserveQualityChunk(&state, []byte(sse(
		`data: {"choices":[{"delta":{"content":"`+strings.Repeat(sentence, 300)+`"}}]}`,
		`data: {"choices":[{"delta":{},"finish_reason":"length"}]}`,
		`data: {"usage":{"completion_tokens":32768,"completion_tokens_details":{"reasoning_tokens":0}}}`,
		"data: [DONE]",
	)))
	sig := state.signals()
	if sig.RepeatMax < 50 {
		t.Fatalf("repeatMax = %d, want >= 50", sig.RepeatMax)
	}
	if sig.HasReasoningDelta || sig.HasThinking {
		t.Fatalf("unexpected thinking evidence: %#v", sig)
	}
	verdict, rule := classifyQualityHold(sig, 8)
	if verdict != QualityWithhold || rule != qualityWithholdRuleDegenerateLoop {
		t.Fatalf("verdict=%v rule=%v sig=%#v", verdict, rule, sig)
	}
}

// TestObserveQualityChunkVariedContent is the negative control: a long answer
// whose sentences all differ (the way a table or checklist differs per row)
// must not be classified as a degenerate loop.
func TestObserveQualityChunkVariedContent(t *testing.T) {
	state := qualityScanState{protocol: qualityProtocolChat}
	var b strings.Builder
	for i := 0; i < 60; i++ {
		fmt.Fprintf(&b, "第 %d 条：编号 %06d 的设备在 %02d:%02d 记录到温度 %d.%d 度，阈值 %d 度，状态正常。", i, i*7919, i%24, (i*13)%60, 20+i%9, i%10, 30+i%5)
	}
	ObserveQualityChunk(&state, []byte(sse(
		`data: {"choices":[{"delta":{"content":"`+b.String()+`"}}]}`,
		`data: {"choices":[{"delta":{},"finish_reason":"stop"}]}`,
		`data: {"usage":{"completion_tokens":900,"completion_tokens_details":{"reasoning_tokens":0}}}`,
		"data: [DONE]",
	)))
	sig := state.signals()
	if qualityIsDegenerateLoop(sig) {
		t.Fatalf("varied content must not trigger the loop rule: %#v", sig)
	}
}
