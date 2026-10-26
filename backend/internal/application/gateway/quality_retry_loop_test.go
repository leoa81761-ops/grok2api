package gateway

import "testing"

func TestQualityIsDegenerateLoop(t *testing.T) {
	base := QualityStreamSignals{VisibleTokens: 9000, RepeatMax: 12}
	if !qualityIsDegenerateLoop(base) {
		t.Fatal("repeated window with no thinking must be withheld")
	}
	cases := []struct {
		name string
		sig  QualityStreamSignals
	}{
		{"below repeat threshold", QualityStreamSignals{VisibleTokens: 9000, RepeatMax: 11}},
		{"too little visible text", QualityStreamSignals{VisibleTokens: 64, RepeatMax: 40}},
		{"plaintext reasoning seen", QualityStreamSignals{VisibleTokens: 9000, RepeatMax: 40, HasReasoningDelta: true}},
		{"cipher thinking seen", QualityStreamSignals{VisibleTokens: 9000, RepeatMax: 40, HasThinking: true}},
		{"tool call present", QualityStreamSignals{VisibleTokens: 9000, RepeatMax: 40, SemanticOutput: true}},
	}
	for _, c := range cases {
		if qualityIsDegenerateLoop(c.sig) {
			t.Fatalf("%s: must not be withheld", c.name)
		}
	}
}

func TestDegenerateLoopWithholdsInClassifier(t *testing.T) {
	verdict, rule := classifyQualityHold(QualityStreamSignals{VisibleTokens: 9000, RepeatMax: 30, Terminal: true}, 8)
	if verdict != QualityWithhold || rule != qualityWithholdRuleDegenerateLoop {
		t.Fatalf("verdict=%v rule=%v", verdict, rule)
	}
}
