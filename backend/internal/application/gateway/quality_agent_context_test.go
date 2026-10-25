package gateway

import "testing"

// TestClassifyQualityHoldEmptyAgentContext 覆盖「大上下文空转收尾」窄规则：
// 线上事故形态是输入 15–18 万 token、输出 33–63 token、reasoning=0、声明了 tools
// 但没有工具调用，却被当成 200 成功交付。该规则必须扣留（触发换号重试），
// 同时不能影响小上下文、带工具调用、带推理或输出足够的正常回合。
func TestClassifyQualityHoldEmptyAgentContext(t *testing.T) {
	base := QualityStreamSignals{
		ToolsDeclared:        true,
		DegradeEmptyToolTurn: true,
		Terminal:             true,
		InputTokens:          160_000,
		OutputTokens:         60,
		VisibleTokens:        0,
		AgentMinInputTokens:  100_000,
		AgentMinOutputTokens: 64,
	}
	cases := []struct {
		name   string
		mutate func(*QualityStreamSignals)
		want   QualityVerdict
	}{
		{"large context empty agent turn is withheld", func(*QualityStreamSignals) {}, QualityWithhold},
		{"small context delivers", func(s *QualityStreamSignals) { s.InputTokens = 5_000 }, QualityDeliver},
		{"tool call delivers", func(s *QualityStreamSignals) { s.SemanticOutput = true }, QualityDeliver},
		{"reasoning delivers", func(s *QualityStreamSignals) { s.HasReasoningDelta = true; s.HasThinking = true }, QualityDeliver},
		{"enough visible output delivers", func(s *QualityStreamSignals) { s.VisibleTokens = 200 }, QualityDeliver},
		{"no tools declared falls back to plain rule", func(s *QualityStreamSignals) { s.ToolsDeclared = false }, QualityWait},
		{"not terminal waits", func(s *QualityStreamSignals) { s.Terminal = false }, QualityWait},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			sig := base
			tc.mutate(&sig)
			if got := ClassifyQualityHold(sig, 8); got != tc.want {
				t.Fatalf("ClassifyQualityHold() = %s, want %s", got, tc.want)
			}
		})
	}
}
