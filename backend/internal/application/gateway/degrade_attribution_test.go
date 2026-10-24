package gateway

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"net/http"
	"path/filepath"
	"strings"
	"testing"
	"time"

	accountapp "github.com/chenyme/grok2api/backend/internal/application/account"
	clientkeyapp "github.com/chenyme/grok2api/backend/internal/application/clientkey"
	accountdomain "github.com/chenyme/grok2api/backend/internal/domain/account"
	"github.com/chenyme/grok2api/backend/internal/domain/clientkey"
	"github.com/chenyme/grok2api/backend/internal/infra/persistence/relational"
	"github.com/chenyme/grok2api/backend/internal/infra/provider"
	"github.com/chenyme/grok2api/backend/internal/infra/runtime/memory"
)

func degradeTestConfig() QualityRetryRuntime {
	return QualityRetryRuntime{
		DegradeAttributionEnabled:        true,
		DegradationMode:                  degradationModeEnforce,
		DegradeWindow:                    30 * time.Minute,
		DegradeMinSamples:                4,
		DegradeAccountThreshold:          0.6,
		DegradeNodeThreshold:             0.8,
		DegradeNodeMinAccounts:           2,
		DegradeMaxAttributionInputTokens: 50000,
		DegradeShapeMinGenerationMS:      500,
		DegradeAccountCooldowns:          []time.Duration{5 * time.Minute, 15 * time.Minute, time.Hour},
		DegradeMaxStrikes:                3,
		DegradeQuarantineDuration:        15 * time.Minute,
		DegradeUnknownCooldown:           2 * time.Minute,
		DegradeGlobalSuppressThreshold:   0.9,
		DegradeEmptyToolTurn:             true,
	}
}

func TestQualityEmptyToolTurnClassification(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name string
		sig  QualityStreamSignals
		want QualityVerdict
	}{
		{
			// 窄规则的命中形态：tools + 无工具调用 + 无推理 + 可见与总量都只有个位数。
			name: "declared tools tiny idle turn withholds",
			sig:  QualityStreamSignals{Terminal: true, ToolsDeclared: true, DegradeEmptyToolTurn: true, VisibleTokens: 3, OutputTokens: 3},
			want: QualityWithhold,
		},
		{
			// 反例 1（lead 指定）：纯文本 9 token >= minOutputTokens 是正常收尾，必须交付。
			name: "declared tools text finish above floor delivers",
			sig:  QualityStreamSignals{Terminal: true, ToolsDeclared: true, DegradeEmptyToolTurn: true, VisibleTokens: 9, OutputTokens: 9},
			want: QualityDeliver,
		},
		{
			// 反例 2：总量超过阈值（usage 报了 40）也不能拦，避免误伤正常收尾。
			name: "declared tools total output above floor delivers",
			sig:  QualityStreamSignals{Terminal: true, ToolsDeclared: true, DegradeEmptyToolTurn: true, VisibleTokens: 3, OutputTokens: 40},
			want: QualityDeliver,
		},
		{
			name: "declared tools long text without reasoning delivers",
			sig:  QualityStreamSignals{Terminal: true, ToolsDeclared: true, DegradeEmptyToolTurn: true, VisibleTokens: 512, OutputTokens: 512},
			want: QualityDeliver,
		},
		{
			name: "declared tools with tool call delivers",
			sig:  QualityStreamSignals{Terminal: true, ToolsDeclared: true, DegradeEmptyToolTurn: true, VisibleTokens: 3, OutputTokens: 3, SemanticOutput: true},
			want: QualityDeliver,
		},
		{
			name: "declared tools with reasoning delivers",
			sig:  QualityStreamSignals{Terminal: true, ToolsDeclared: true, DegradeEmptyToolTurn: true, VisibleTokens: 3, OutputTokens: 3, HasThinking: true},
			want: QualityDeliver,
		},
		{
			name: "declared tools with usage reasoning tokens delivers",
			sig:  QualityStreamSignals{Terminal: true, ToolsDeclared: true, DegradeEmptyToolTurn: true, VisibleTokens: 3, OutputTokens: 3, ReasoningTokens: 20},
			want: QualityDeliver,
		},
		{
			// 纯聊天请求不声明 tools，短回答不受影响。
			name: "chat without tools keeps short reply",
			sig:  QualityStreamSignals{Terminal: true, DegradeEmptyToolTurn: true, VisibleTokens: 3, OutputTokens: 3},
			want: QualityDeliver,
		},
		{
			name: "switch off keeps short tool turn",
			sig:  QualityStreamSignals{Terminal: true, ToolsDeclared: true, VisibleTokens: 3, OutputTokens: 3},
			want: QualityDeliver,
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			if got := ClassifyQualityHold(test.sig, 8); got != test.want {
				t.Fatalf("ClassifyQualityHold() = %s, want %s (%#v)", got, test.want, test.sig)
			}
			wantRule := test.name == "declared tools tiny idle turn withholds"
			if test.sig.ToolsDeclared && test.sig.DegradeEmptyToolTurn {
				if got := qualityIsEmptyToolTurn(test.sig, 8); got != wantRule {
					t.Fatalf("qualityIsEmptyToolTurn() = %t, want %t (%#v)", got, wantRule, test.sig)
				}
			}
		})
	}
}

func TestQualityRequestDeclaresTools(t *testing.T) {
	t.Parallel()
	for _, test := range []struct {
		name string
		body string
		want bool
	}{
		{name: "tools array", body: `{"tools":[{"type":"function","name":"read"}]}`, want: true},
		{name: "empty tools", body: `{"tools":[]}`, want: false},
		{name: "no tools", body: `{"messages":[]}`, want: false},
		{name: "invalid json", body: `{`, want: false},
	} {
		t.Run(test.name, func(t *testing.T) {
			if got := qualityRequestDeclaresTools([]byte(test.body)); got != test.want {
				t.Fatalf("qualityRequestDeclaresTools(%s) = %t, want %t", test.body, got, test.want)
			}
		})
	}
}

func TestClassifyDegradationHitLayers(t *testing.T) {
	t.Parallel()
	cfg := degradeTestConfig()
	for _, test := range []struct {
		name string
		hit  degradationHit
		want degradationHitClass
	}{
		// 标签命中（withhold / 上游截断）是网关自身结论，一律计入统计：
		// hold 掉的流没有首字时间，gen_window=0 是「不可测」而不是伪影。
		{name: "withheld stream without first token is eligible", hit: degradationHit{source: degradationSourceQualityWithhold, genWindowMS: 0}, want: degradationHitEligible},
		{name: "withheld stream without measurable window is eligible", hit: degradationHit{source: degradationSourceQualityWithhold, genWindowMS: -1}, want: degradationHitEligible},
		{name: "withheld large context still eligible", hit: degradationHit{source: degradationSourceQualityWithhold, inputTokens: 130936, genWindowMS: 0}, want: degradationHitEligible},
		{name: "truncated large context still eligible", hit: degradationHit{source: degradationSourceUpstreamTruncation, inputTokens: 130000, genWindowMS: 0}, want: degradationHitEligible},
		// 只有空转窄规则形状命中才按测量值分层。
		{name: "empty tool turn short window is shape", hit: degradationHit{source: degradationSourceAgentEmptyToolTurn, inputTokens: 1000, genWindowMS: 200}, want: degradationHitShape},
		{name: "empty tool turn large context is context", hit: degradationHit{source: degradationSourceAgentEmptyToolTurn, inputTokens: 60000, genWindowMS: 5000}, want: degradationHitContext},
		{name: "empty tool turn normal window eligible", hit: degradationHit{source: degradationSourceAgentEmptyToolTurn, inputTokens: 1000, genWindowMS: 900}, want: degradationHitEligible},
		{name: "empty tool turn unknown window eligible", hit: degradationHit{source: degradationSourceAgentEmptyToolTurn, inputTokens: 1000, genWindowMS: -1}, want: degradationHitEligible},
	} {
		t.Run(test.name, func(t *testing.T) {
			if got := cfg.classifyDegradationHit(test.hit); got != test.want {
				t.Fatalf("classifyDegradationHit(%#v) = %v, want %v", test.hit, got, test.want)
			}
		})
	}
}

func TestWithholdSourceSelectionFromRule(t *testing.T) {
	t.Parallel()
	credential := accountdomain.Credential{ID: 1, Provider: accountdomain.ProviderBuild}
	cases := []struct {
		rule qualityWithholdRule
		want degradationSource
	}{
		{rule: qualityWithholdRuleEmptyToolTurn, want: degradationSourceAgentEmptyToolTurn},
		{rule: qualityWithholdRuleMissingThinking, want: degradationSourceQualityWithhold},
		{rule: qualityWithholdRuleBurstDump, want: degradationSourceQualityWithhold},
		{rule: qualityWithholdRuleNone, want: degradationSourceQualityWithhold},
	}
	for _, test := range cases {
		hit := newWithholdDegradationHit(credential, nil, accountdomain.ProviderBuild, Usage{}, QualityStreamEvidence{WithholdRule: test.rule})
		if hit.source != test.want {
			t.Fatalf("rule %q -> source %q, want %q", test.rule, hit.source, test.want)
		}
	}
}

// peek 要把 Withhold 的触发规则回传给归因层，否则无法区分标签命中与空转窄规则。
func TestPeekReportsWithholdRule(t *testing.T) {
	t.Parallel()
	cfg := QualityRetryRuntime{MinOutputTokens: 8, HoldTimeout: time.Second, DegradeEmptyToolTurn: true}
	// 空转窄规则：tools 声明 + 3 token 纯文本 + 无工具调用 + 无推理。
	tiny := sse(`data: {"choices":[{"delta":{"content":"hi."}}]}`,
		`data: {"usage":{"prompt_tokens":10,"completion_tokens":2,"completion_tokens_details":{"reasoning_tokens":0}}}`,
		"data: [DONE]")
	options := &QualityPeekOptions{ToolsDeclared: true}
	_, verdict, _, _, err := peekQualityStream(context.Background(), io.NopCloser(strings.NewReader(tiny)), qualityProtocolChat, cfg, options)
	if err != nil {
		t.Fatal(err)
	}
	if verdict != QualityWithhold || options.Evidence.WithholdRule != qualityWithholdRuleEmptyToolTurn {
		t.Fatalf("tiny tool turn: verdict=%s rule=%q", verdict, options.Evidence.WithholdRule)
	}

	// missing-thinking：没有 tools 的长文本无推理，应回传 missing_thinking 规则。
	long := sse(`data: {"choices":[{"delta":{"content":"`+strings.Repeat("abcd", 40)+`"}}]}`,
		`data: {"usage":{"prompt_tokens":10,"completion_tokens":40,"completion_tokens_details":{"reasoning_tokens":0}}}`,
		"data: [DONE]")
	options = &QualityPeekOptions{}
	_, verdict, _, _, err = peekQualityStream(context.Background(), io.NopCloser(strings.NewReader(long)), qualityProtocolChat, cfg, options)
	if err != nil {
		t.Fatal(err)
	}
	if verdict != QualityWithhold || options.Evidence.WithholdRule != qualityWithholdRuleMissingThinking {
		t.Fatalf("missing thinking: verdict=%s rule=%q", verdict, options.Evidence.WithholdRule)
	}
}

func TestDegradationAttributionUnknownWithoutEnoughSamples(t *testing.T) {
	t.Parallel()
	cfg := degradeTestConfig()
	tracker := newDegradationTracker()
	evidence := tracker.recordDegradedAndDecide(degradationHit{accountID: 1, nodeID: 7, inputTokens: 1000}, cfg)
	if evidence.decision != degradationReasonUnknown {
		t.Fatalf("single degraded sample must stay unknown, got %#v", evidence)
	}
}

func TestDegradationAttributionSingleNodeAccountStillAccount(t *testing.T) {
	t.Parallel()
	cfg := degradeTestConfig()
	tracker := newDegradationTracker()
	for range cfg.DegradeMinSamples {
		tracker.recordDegradedAndDecide(degradationHit{accountID: 31, nodeID: 301, inputTokens: 1000}, cfg)
	}
	evidence := tracker.recordDegradedAndDecide(degradationHit{accountID: 31, nodeID: 301, inputTokens: 1000}, cfg)
	// 单节点账号证据：先判 ACCOUNT（处置第一档只隔离当前节点租约），升级要等跨节点。
	if evidence.decision != degradationReasonAccount || evidence.accountBadNodes != 1 {
		t.Fatalf("single-node account evidence must be account rung 1, got %#v", evidence)
	}
}

func TestDegradationAttributionAccountDecisionAcrossNodes(t *testing.T) {
	t.Parallel()
	cfg := degradeTestConfig()
	tracker := newDegradationTracker()
	for range cfg.DegradeMinSamples {
		tracker.recordDegradedAndDecide(degradationHit{accountID: 11, nodeID: 101, inputTokens: 1000}, cfg)
	}
	for range cfg.DegradeMinSamples {
		tracker.recordDegradedAndDecide(degradationHit{accountID: 11, nodeID: 102, inputTokens: 1000}, cfg)
	}
	evidence := tracker.recordDegradedAndDecide(degradationHit{accountID: 11, nodeID: 102, inputTokens: 1000}, cfg)
	if evidence.decision != degradationReasonAccount {
		t.Fatalf("account degrading on two nodes must be account-attributed, got %#v", evidence)
	}
	if evidence.accountBadNodes < 2 {
		t.Fatalf("account_bad_nodes = %d, want >= 2", evidence.accountBadNodes)
	}
}

func TestDegradationAttributionNodeDecisionSharedNode(t *testing.T) {
	t.Parallel()
	cfg := degradeTestConfig()
	tracker := newDegradationTracker()
	// 同一节点上两个账号都退化：账号自身只有单节点证据，问题指向出口。
	for _, accountID := range []uint64{21, 22} {
		for range cfg.DegradeMinSamples {
			tracker.recordDegradedAndDecide(degradationHit{accountID: accountID, nodeID: 201, inputTokens: 1000}, cfg)
		}
	}
	evidence := tracker.recordDegradedAndDecide(degradationHit{accountID: 21, nodeID: 201, inputTokens: 1000}, cfg)
	if evidence.decision != degradationReasonNode {
		t.Fatalf("shared degraded node must be node-attributed, got %#v", evidence)
	}
	if evidence.nodeAccounts < 2 || evidence.nodeAccountsDegraded < 2 {
		t.Fatalf("node accounts = %d/%d, want >= 2 degraded", evidence.nodeAccountsDegraded, evidence.nodeAccounts)
	}
}

func TestDegradationAttributionGlobalSuppression(t *testing.T) {
	t.Parallel()
	cfg := degradeTestConfig()
	tracker := newDegradationTracker()
	// 六个账号各自在不同节点全部退化：全局样本 >= 4*minSamples 且 100% 退化，
	// 说明是上游整体故障，应抑制逐账号处置。
	for accountID := uint64(41); accountID <= 46; accountID++ {
		for range cfg.DegradeMinSamples {
			tracker.recordDegradedAndDecide(degradationHit{accountID: accountID, nodeID: accountID + 400, inputTokens: 1000}, cfg)
		}
	}
	evidence := tracker.recordDegradedAndDecide(degradationHit{accountID: 47, nodeID: 500, inputTokens: 1000}, cfg)
	if !evidence.globalSuppressed || evidence.decision != degradationReasonUnknown {
		t.Fatalf("global degradation must suppress attribution, got %#v", evidence)
	}
}

func TestDegradationAttributionWindowExpiryAndHealthyDenominator(t *testing.T) {
	t.Parallel()
	cfg := degradeTestConfig()
	tracker := newDegradationTracker()
	started := time.Now().UTC().Add(-2 * cfg.DegradeWindow)
	for range cfg.DegradeMinSamples {
		tracker.recordDegradedAndDecide(degradationHit{accountID: 41, nodeID: 401, inputTokens: 1000, at: started}, cfg)
	}
	// 窗口已过期：旧样本全部被裁掉，归因退回 UNKNOWN。
	evidence := tracker.recordDegradedAndDecide(degradationHit{accountID: 41, nodeID: 401, inputTokens: 1000}, cfg)
	if evidence.accountTotal != 1 || evidence.decision != degradationReasonUnknown {
		t.Fatalf("expired samples must be dropped, got %#v", evidence)
	}

	// 正常样本进入分母：4 退化 + 4 正常 = 0.5 < accThreshold 0.6。
	for range cfg.DegradeMinSamples {
		tracker.record(degradationHit{accountID: 42, nodeID: 402, inputTokens: 1000}, false, cfg)
	}
	for range cfg.DegradeMinSamples {
		tracker.recordDegradedAndDecide(degradationHit{accountID: 42, nodeID: 402, inputTokens: 1000}, cfg)
	}
	evidence = tracker.recordDegradedAndDecide(degradationHit{accountID: 42, nodeID: 402, inputTokens: 1000}, cfg)
	if evidence.accountTotal != 9 || evidence.accountRate >= cfg.DegradeAccountThreshold {
		t.Fatalf("healthy samples must stay in denominator, got %#v", evidence)
	}
}

func TestDegradationStrikeLadder(t *testing.T) {
	t.Parallel()
	cfg := degradeTestConfig()
	if got := cfg.degradeLadderCooldown(1); got != 5*time.Minute {
		t.Fatalf("strike 1 = %s", got)
	}
	if got := cfg.degradeLadderCooldown(2); got != 15*time.Minute {
		t.Fatalf("strike 2 = %s", got)
	}
	if got := cfg.degradeLadderCooldown(3); got != time.Hour {
		t.Fatalf("strike 3 = %s", got)
	}
	if got := cfg.degradeLadderCooldown(9); got != time.Hour {
		t.Fatalf("strike beyond ladder must repeat last rung, got %s", got)
	}
	tracker := newDegradationTracker()
	if got := tracker.bumpAccountStrikes(51); got != 1 {
		t.Fatalf("bump = %d", got)
	}
	if got := tracker.bumpAccountStrikes(51); got != 2 {
		t.Fatalf("bump = %d", got)
	}
	tracker.clearAccountStrikes(51)
	if got := tracker.accountStrikes(51); got != 0 {
		t.Fatalf("strike ladder not cleared: %d", got)
	}
}

func degradeTestService(t *testing.T) (*Service, *Selector, *relational.AccountRepository, accountdomain.Credential) {
	t.Helper()
	ctx := context.Background()
	database, err := relational.OpenSQLite(ctx, filepath.Join(t.TempDir(), "degrade.db"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = database.Close() })
	if err := database.InitializeSchema(ctx); err != nil {
		t.Fatal(err)
	}
	accountRepo := relational.NewAccountRepository(database)
	auditRepo := relational.NewAuditRepository(database)
	adapter := &scriptedBuildAdapter{}
	registry := provider.NewRegistry(adapter)
	sticky := memory.NewStickyStore()
	accountService := accountapp.NewService(accountRepo, auditRepo, memory.NewDeviceSessionStore(), sticky, registry, testCipher(t), nil)
	selector := NewSelector(accountRepo, memory.NewConcurrencyLimiter(), sticky, registry, time.Hour, time.Second, time.Minute)
	service := NewService(nil, nil, accountService, nil, registry, selector, nil, 3)
	service.UpdateQualityRetry(degradeTestConfig())
	credential, _, err := accountRepo.UpsertByIdentity(ctx, accountdomain.Credential{
		Provider: accountdomain.ProviderBuild, Name: "degrade-account", SourceKey: "degrade-account",
		EncryptedAccessToken: "access", EncryptedRefreshToken: "refresh",
		ExpiresAt: time.Now().Add(time.Hour), Enabled: true, AuthStatus: accountdomain.AuthStatusActive,
		Priority: 100, MaxConcurrent: 1,
	})
	if err != nil {
		t.Fatal(err)
	}
	return service, selector, accountRepo, credential
}

// recordingLeaseQuarantiner 记录账号×节点租约隔离写入。
type recordingLeaseQuarantiner struct {
	blocks []accountdomain.EgressLeaseBlock
	err    error
}

func (r *recordingLeaseQuarantiner) UpsertEgressLeaseBlock(_ context.Context, value accountdomain.EgressLeaseBlock) (accountdomain.EgressLeaseBlock, error) {
	if r.err != nil {
		return accountdomain.EgressLeaseBlock{}, r.err
	}
	r.blocks = append(r.blocks, value)
	return value, nil
}

// recordingNodeDisabler 记录节点调度开关调用。
type recordingNodeDisabler struct {
	calls   [][]uint64
	enabled []bool
	updated int
	err     error
}

func (r *recordingNodeDisabler) UpdateManyEnabled(_ context.Context, nodeIDs []uint64, enabled bool) (int, error) {
	r.calls = append(r.calls, append([]uint64(nil), nodeIDs...))
	r.enabled = append(r.enabled, enabled)
	if r.err != nil {
		return 0, r.err
	}
	return r.updated, nil
}

func TestDegradationPenaltyAccountFirstRungQuarantinesLease(t *testing.T) {
	ctx := context.Background()
	service, _, accountRepo, credential := degradeTestService(t)
	quarantiner := &recordingLeaseQuarantiner{}
	service.SetEgressLeaseQuarantiner(quarantiner)

	action, _, err := service.degradationPenalty(ctx, credential, degradationHit{accountID: credential.ID, nodeID: 7}, degradationEvidence{decision: degradationReasonAccount, accountBadNodes: 1}, service.qualityRetryConfig())
	if err != nil || action != degradationActionLeaseQuarantined {
		t.Fatalf("account rung 1 action = %q, err = %v", action, err)
	}
	if len(quarantiner.blocks) != 1 {
		t.Fatalf("lease quarantine not written: %#v", quarantiner.blocks)
	}
	block := quarantiner.blocks[0]
	if block.AccountID != credential.ID || block.NodeID != 7 || block.Reason != "degraded_account" || block.Version == "" {
		t.Fatalf("unexpected lease block: %#v", block)
	}
	if remaining := time.Until(block.CooldownUntil); remaining < 14*time.Minute || remaining > 16*time.Minute {
		t.Fatalf("lease quarantine = %s, want about 15m", remaining)
	}
	account, err := accountRepo.Get(ctx, credential.ID)
	if err != nil {
		t.Fatal(err)
	}
	if !account.Enabled || account.CooldownUntil != nil || account.LastError != "" {
		t.Fatalf("first rung must not punish the account: %#v", account)
	}
	if service.degrade.accountStrikes(credential.ID) != 0 {
		t.Fatal("first rung must not consume a strike")
	}
}

func TestDegradationPenaltyAccountEscalatesAfterTwoNodes(t *testing.T) {
	ctx := context.Background()
	service, _, accountRepo, credential := degradeTestService(t)
	quarantiner := &recordingLeaseQuarantiner{}
	service.SetEgressLeaseQuarantiner(quarantiner)

	// 该账号已经在另一个节点被判定过一次 ACCOUNT，本次第二个节点触发升级。
	service.degrade.noteAccountNodeJudgement(credential.ID, 8, time.Now().UTC(), time.Hour)
	action, _, err := service.degradationPenalty(ctx, credential, degradationHit{accountID: credential.ID, nodeID: 7}, degradationEvidence{decision: degradationReasonAccount, accountBadNodes: 2}, service.qualityRetryConfig())
	if err != nil || action != degradationActionAccountCooldown {
		t.Fatalf("escalated action = %q, err = %v", action, err)
	}
	if len(quarantiner.blocks) != 0 {
		t.Fatal("escalated account must use account cooldown, not lease quarantine")
	}
	account, err := accountRepo.Get(ctx, credential.ID)
	if err != nil {
		t.Fatal(err)
	}
	if !account.Enabled || account.CooldownUntil == nil || account.LastError != accountdomain.LastErrorDegradedAccount {
		t.Fatalf("escalated account not cooled: %#v", account)
	}
	if remaining := time.Until(*account.CooldownUntil); remaining < 4*time.Minute || remaining > 6*time.Minute {
		t.Fatalf("first ladder rung = %s, want about 5m", remaining)
	}
	if service.degrade.accountStrikes(credential.ID) != 1 {
		t.Fatalf("strikes = %d, want 1", service.degrade.accountStrikes(credential.ID))
	}
}

func TestDegradationPenaltyNodeDisablesSchedulingWithoutHealthWrite(t *testing.T) {
	ctx := context.Background()
	service, _, accountRepo, credential := degradeTestService(t)
	disabler := &recordingNodeDisabler{updated: 1}
	lease := &recordingLeaseQuarantiner{}
	service.SetEgressNodeDisabler(disabler)
	service.SetEgressLeaseQuarantiner(lease)
	service.degrade.bumpAccountStrikes(credential.ID)

	action, _, err := service.degradationPenalty(ctx, credential, degradationHit{accountID: credential.ID, nodeID: 7}, degradationEvidence{decision: degradationReasonNode}, service.qualityRetryConfig())
	if err != nil || action != degradationActionNodeDisabled {
		t.Fatalf("node action = %q, err = %v", action, err)
	}
	if len(disabler.calls) != 1 || len(disabler.calls[0]) != 1 || disabler.calls[0][0] != 7 || disabler.enabled[0] {
		t.Fatalf("node scheduling not disabled: %#v %#v", disabler.calls, disabler.enabled)
	}
	if len(lease.blocks) != 0 {
		t.Fatal("disabled node must not also quarantine a lease")
	}
	if service.degrade.accountStrikes(credential.ID) != 0 {
		t.Fatal("node attribution must clear the account strike ladder")
	}
	account, err := accountRepo.Get(ctx, credential.ID)
	if err != nil {
		t.Fatal(err)
	}
	if !account.Enabled || account.CooldownUntil != nil || account.LastError != "" {
		t.Fatalf("node attribution must not punish the account: %#v", account)
	}
}

func TestDegradationPenaltyNodeFallsBackToLeaseQuarantine(t *testing.T) {
	ctx := context.Background()
	service, _, _, credential := degradeTestService(t)
	disabler := &recordingNodeDisabler{updated: 0}
	lease := &recordingLeaseQuarantiner{}
	service.SetEgressNodeDisabler(disabler)
	service.SetEgressLeaseQuarantiner(lease)

	action, _, err := service.degradationPenalty(ctx, credential, degradationHit{accountID: credential.ID, nodeID: 7}, degradationEvidence{decision: degradationReasonNode}, service.qualityRetryConfig())
	if err != nil || action != degradationActionLeaseQuarantined {
		t.Fatalf("node fallback action = %q, err = %v", action, err)
	}
	if len(lease.blocks) != 1 || lease.blocks[0].Reason != "degraded_node" {
		t.Fatalf("node fallback lease not written: %#v", lease.blocks)
	}
}

func TestDegradationPenaltyUnknownOnlyShortCooldown(t *testing.T) {
	ctx := context.Background()
	service, _, accountRepo, credential := degradeTestService(t)
	action, _, err := service.degradationPenalty(ctx, credential, degradationHit{accountID: credential.ID}, degradationEvidence{decision: degradationReasonUnknown}, service.qualityRetryConfig())
	if err != nil || action != degradationActionAccountCooldown {
		t.Fatalf("unknown action = %q, err = %v", action, err)
	}
	account, err := accountRepo.Get(ctx, credential.ID)
	if err != nil {
		t.Fatal(err)
	}
	if !account.Enabled || account.CooldownUntil == nil || account.LastError != accountdomain.LastErrorDegradedAccount {
		t.Fatalf("unknown decision must only cool briefly: %#v", account)
	}
	if remaining := time.Until(*account.CooldownUntil); remaining < 1*time.Minute || remaining > 3*time.Minute {
		t.Fatalf("unknown cooldown = %s, want about 2m", remaining)
	}
	if service.degrade.accountStrikes(credential.ID) != 0 {
		t.Fatal("unknown decision must not consume a strike")
	}
}

func TestObserveModeNeverAppliesPenalty(t *testing.T) {
	ctx := context.Background()
	service, _, accountRepo, credential := degradeTestService(t)
	observe := service.qualityRetryConfig()
	observe.DegradationMode = degradationModeObserve
	lease := &recordingLeaseQuarantiner{}
	disabler := &recordingNodeDisabler{updated: 1}
	service.SetEgressLeaseQuarantiner(lease)
	service.SetEgressNodeDisabler(disabler)
	service.degrade.bumpAccountStrikes(credential.ID)

	for _, test := range []struct {
		name     string
		evidence degradationEvidence
		hit      degradationHit
		would    string
	}{
		{name: "account first rung", evidence: degradationEvidence{decision: degradationReasonAccount, accountBadNodes: 1}, hit: degradationHit{accountID: credential.ID, nodeID: 7}, would: degradationActionLeaseQuarantined},
		{name: "account escalated", evidence: degradationEvidence{decision: degradationReasonAccount, accountBadNodes: 2}, hit: degradationHit{accountID: credential.ID, nodeID: 7}, would: degradationActionAccountCooldown},
		{name: "node", evidence: degradationEvidence{decision: degradationReasonNode}, hit: degradationHit{accountID: credential.ID, nodeID: 7}, would: degradationActionNodeDisabled},
		{name: "unknown", evidence: degradationEvidence{decision: degradationReasonUnknown}, hit: degradationHit{accountID: credential.ID}, would: degradationActionAccountCooldown},
	} {
		t.Run(test.name, func(t *testing.T) {
			action, wouldAction, err := service.degradationPenalty(ctx, credential, test.hit, test.evidence, observe)
			if err != nil {
				t.Fatalf("observe penalty err = %v", err)
			}
			if action != degradationActionObserveOnly {
				t.Fatalf("observe action = %q, want %q", action, degradationActionObserveOnly)
			}
			if wouldAction != test.would {
				t.Fatalf("would_action = %q, want %q", wouldAction, test.would)
			}
		})
	}
	if len(lease.blocks) != 0 || len(disabler.calls) != 0 {
		t.Fatalf("observe mode must not quarantine leases or disable nodes: %#v %#v", lease.blocks, disabler.calls)
	}
	account, err := accountRepo.Get(ctx, credential.ID)
	if err != nil {
		t.Fatal(err)
	}
	if !account.Enabled || account.CooldownUntil != nil || account.LastError != "" {
		t.Fatalf("observe mode must not punish the account: %#v", account)
	}
	if service.degrade.accountStrikes(credential.ID) != 1 {
		t.Fatalf("observe mode must not consume strikes, got %d", service.degrade.accountStrikes(credential.ID))
	}
	// observe 也不应写入跨节点判定记录（否则会影响后续 enforce 判定）。
	if got := service.degrade.accountNodeJudgementPlan(credential.ID, 7, time.Now().UTC(), time.Hour); got != 1 {
		t.Fatalf("observe mode must not record account-node judgements, got %d", got)
	}
}

func TestObserveModeStillComputesAttributionEvidence(t *testing.T) {
	ctx := context.Background()
	service, _, accountRepo, credential := degradeTestService(t)
	observe := service.qualityRetryConfig()
	observe.DegradationMode = degradationModeObserve
	service.SetEgressLeaseQuarantiner(&recordingLeaseQuarantiner{})
	service.UpdateQualityRetry(observe)

	// 灌够账号阈值样本，观察模式仍要照常累计统计并输出 reason=account。
	for range observe.DegradeMinSamples + 1 {
		service.observeDegradationHit(ctx, "req-observe", credential, degradationHit{accountID: credential.ID, nodeID: 7, inputTokens: 1000, genWindowMS: 900}, Usage{InputTokens: 1000}, observe)
	}
	account, err := accountRepo.Get(ctx, credential.ID)
	if err != nil {
		t.Fatal(err)
	}
	if !account.Enabled || account.CooldownUntil != nil {
		t.Fatalf("observe mode must not apply account cooldown: %#v", account)
	}
	evidence := service.degrade.recordDegradedAndDecide(degradationHit{accountID: credential.ID, nodeID: 7, inputTokens: 1000}, observe)
	if evidence.accountTotal < observe.DegradeMinSamples {
		t.Fatalf("observe mode must keep counting samples, got %#v", evidence)
	}
}

func TestDegradationHealthySampleResetsStrikeLadder(t *testing.T) {
	service := &Service{degrade: newDegradationTracker(), logger: slog.Default()}
	service.UpdateQualityRetry(degradeTestConfig())
	credential := accountdomain.Credential{ID: 61, Provider: accountdomain.ProviderBuild}
	service.degrade.bumpAccountStrikes(credential.ID)
	service.observeHealthyQualitySample(credential, nil, accountdomain.ProviderBuild, Usage{InputTokens: 1000}, QualityStreamEvidence{})
	if service.degrade.accountStrikes(credential.ID) != 1 {
		t.Fatal("不带推理证据的正常样本不能清零阶梯")
	}
	service.observeHealthyQualitySample(credential, nil, accountdomain.ProviderBuild, Usage{InputTokens: 1000}, QualityStreamEvidence{HasThinking: true})
	if service.degrade.accountStrikes(credential.ID) != 0 {
		t.Fatal("带推理证据的成功必须清零冷却阶梯")
	}
}

func TestObserveDegradationHitSkipsContextAndShape(t *testing.T) {
	ctx := context.Background()
	service := &Service{degrade: newDegradationTracker(), logger: slog.Default()}
	service.UpdateQualityRetry(degradeTestConfig())
	credential := accountdomain.Credential{ID: 71, Provider: accountdomain.ProviderBuild}
	cfg := service.qualityRetryConfig()

	// 空转窄规则（形状命中）才被 SHAPE/CONTEXT 过滤。
	shape := degradationHit{source: degradationSourceAgentEmptyToolTurn, accountID: credential.ID, nodeID: 7, inputTokens: 1000, genWindowMS: 100}
	if reason := service.observeDegradationHit(ctx, "req-shape", credential, shape, Usage{}, cfg); reason != degradationReasonShape {
		t.Fatalf("shape hit = %q", reason)
	}
	if service.degrade.accountStrikes(credential.ID) != 0 || len(service.degrade.accounts) != 0 {
		t.Fatal("shape hit must not enter attribution statistics")
	}
	contextHit := degradationHit{source: degradationSourceAgentEmptyToolTurn, accountID: credential.ID, nodeID: 7, inputTokens: 60000, genWindowMS: 5000}
	if reason := service.observeDegradationHit(ctx, "req-context", credential, contextHit, Usage{}, cfg); reason != degradationReasonContext {
		t.Fatalf("context hit = %q", reason)
	}
	largeContext := degradationHit{source: degradationSourceAgentEmptyToolTurn, accountID: credential.ID, nodeID: 7, inputTokens: 204675, genWindowMS: 5000}
	if reason := service.observeDegradationHit(ctx, "req-context-large", credential, largeContext, Usage{}, cfg); reason != degradationReasonContext {
		t.Fatalf("large context hit = %q", reason)
	}
	if len(service.degrade.accounts) != 0 {
		t.Fatal("context hit must not enter attribution statistics")
	}

	// 标签命中（withhold）没有首字时间、gen_window=0，也必须计入统计，
	// 否则灰度期收集不到任何降智主信号。这里用 observe 模式避免触发处置副作用
	// （本测试只验证分层，不验证处置）。
	withheldCfg := cfg
	withheldCfg.DegradationMode = degradationModeObserve
	withheld := degradationHit{source: degradationSourceQualityWithhold, accountID: credential.ID, nodeID: 7, genWindowMS: 0}
	if reason := service.observeDegradationHit(ctx, "req-withheld", credential, withheld, Usage{}, withheldCfg); reason == degradationReasonShape || reason == degradationReasonContext {
		t.Fatalf("withheld label hit must not be skipped, got reason=%q", reason)
	}
	if len(service.degrade.accounts) != 1 {
		t.Fatalf("withheld label hit must enter attribution statistics, accounts=%d", len(service.degrade.accounts))
	}
}

func TestDegradeAttributionFailClosedNeverDeliversDegradedTwoHundred(t *testing.T) {
	ctx := context.Background()
	database, err := relational.OpenSQLite(ctx, filepath.Join(t.TempDir(), "degrade-fail-closed.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer database.Close()
	if err := database.InitializeSchema(ctx); err != nil {
		t.Fatal(err)
	}
	accountRepo := relational.NewAccountRepository(database)
	modelRepo := relational.NewModelRepository(database)
	auditRepo := relational.NewAuditRepository(database)
	responseRepo := relational.NewResponseRepository(database)
	keyRepo := relational.NewClientKeyRepository(database)
	credential, _, err := accountRepo.UpsertByIdentity(ctx, accountdomain.Credential{
		Provider: accountdomain.ProviderBuild, Name: "degrade-closed", SourceKey: "degrade-closed",
		EncryptedAccessToken: "degrade-closed", EncryptedRefreshToken: "refresh-degrade-closed",
		ExpiresAt: time.Now().Add(time.Hour), Enabled: true, AuthStatus: accountdomain.AuthStatusActive,
		Priority: 200, MaxConcurrent: 1,
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := modelRepo.UpsertDiscovered(ctx, accountdomain.ProviderBuild, []string{"grok-4.6"}); err != nil {
		t.Fatal(err)
	}
	if err := modelRepo.ReplaceAccountCapabilities(ctx, credential.ID, []string{"grok-4.6"}, time.Now().UTC()); err != nil {
		t.Fatal(err)
	}
	clientKey, err := keyRepo.Create(ctx, clientkey.Key{
		Name: "degrade-closed-key", Prefix: "dqclosed", SecretHash: strings.Repeat("a", 64), EncryptedSecret: "encrypted",
		Enabled: true, RPMLimit: 120, MaxConcurrent: 8,
	})
	if err != nil {
		t.Fatal(err)
	}
	noThink := sse(
		`data: {"choices":[{"delta":{"content":"`+strings.Repeat("abcd", 40)+`"}}]}`,
		`data: {"usage":{"prompt_tokens":1000,"completion_tokens":40,"completion_tokens_details":{"reasoning_tokens":0}}}`,
		"data: [DONE]",
	)
	adapter := &scriptedBuildAdapter{responses: map[uint64][]scriptedBuildResponse{
		credential.ID: {{status: http.StatusOK, body: noThink}},
	}}
	registry := provider.NewRegistry(adapter)
	sticky := memory.NewStickyStore()
	accountService := accountapp.NewService(accountRepo, auditRepo, memory.NewDeviceSessionStore(), sticky, registry, testCipher(t), nil)
	selector := NewSelector(accountRepo, memory.NewConcurrencyLimiter(), sticky, registry, time.Hour, time.Second, time.Minute)
	service := NewService(modelRepo, auditRepo, accountService, clientkeyapp.NewService(nil, nil, nil, 60, 4, nil), registry, selector, responseRepo, 3)
	runtime := degradeTestConfig()
	runtime.Enabled = true
	runtime.MaxAttempts = 2
	runtime.MinOutputTokens = 32
	runtime.HoldTimeout = time.Second
	// 即使 YAML 配了 fail_open，归因模式也必须把退化响应升级为 503。
	runtime.OnExhausted = qualityRetryFailOpen
	service.UpdateQualityRetry(runtime)

	result, err := service.CreateChatCompletion(ctx, Input{
		RequestID: "req-degrade-closed", ClientKey: clientKey, PublicModel: "grok-4.6", Streaming: true,
		Body: []byte(`{"model":"grok-4.6","messages":[{"role":"user","content":"write a game"}],"stream":true}`),
	})
	if result != nil {
		t.Fatalf("degraded response must never be delivered as 200, got status %d", result.StatusCode)
	}
	var upstreamFailure *UpstreamFailure
	if !errors.As(err, &upstreamFailure) {
		t.Fatalf("expected UpstreamFailure, got %v", err)
	}
	if upstreamFailure.HTTPStatus != http.StatusServiceUnavailable || upstreamFailure.Code != ErrorQualityDegraded {
		t.Fatalf("failure = %d/%s, want 503/%s", upstreamFailure.HTTPStatus, upstreamFailure.Code, ErrorQualityDegraded)
	}
	if attempts := adapter.Attempts(); len(attempts) != 1 {
		t.Fatalf("single degraded account must not fake a retry, attempts=%#v", attempts)
	}
	account, err := accountRepo.Get(ctx, credential.ID)
	if err != nil {
		t.Fatal(err)
	}
	// 守卫 Withhold 是标签命中：即使测试 SSE 瞬间完成（gen_window≈0），
	// 也必须进入统计。单次样本只能归因到 UNKNOWN → 2m 短冷却，不禁用账号。
	if !account.Enabled || account.LastError != accountdomain.LastErrorDegradedAccount {
		t.Fatalf("withheld label hit must apply UNKNOWN short cooldown, not disable: %#v", account)
	}
	if account.CooldownUntil == nil || time.Until(*account.CooldownUntil) > 3*time.Minute {
		t.Fatalf("withheld label hit must apply short cooldown, got %#v", account)
	}
}

// 大上下文不是失败原因（实测上游没有 128k 硬上限）：标签命中（withhold）即使
// input 很大也照常计入统计、照常换号重试，绝不能 503 context_window_exceeded，
// 也不能因为上下文大就丢样本（由全局抑制线兜底）。
func TestLargeContextStillRetriesAndNeverPunishesAccounts(t *testing.T) {
	ctx := context.Background()
	database, err := relational.OpenSQLite(ctx, filepath.Join(t.TempDir(), "degrade-context.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer database.Close()
	if err := database.InitializeSchema(ctx); err != nil {
		t.Fatal(err)
	}
	accountRepo := relational.NewAccountRepository(database)
	modelRepo := relational.NewModelRepository(database)
	auditRepo := relational.NewAuditRepository(database)
	responseRepo := relational.NewResponseRepository(database)
	keyRepo := relational.NewClientKeyRepository(database)
	credentials := make([]accountdomain.Credential, 0, 2)
	for index, name := range []string{"context-guard-a", "context-guard-b"} {
		credential, _, createErr := accountRepo.UpsertByIdentity(ctx, accountdomain.Credential{
			Provider: accountdomain.ProviderBuild, Name: name, SourceKey: name,
			EncryptedAccessToken: name, EncryptedRefreshToken: "refresh-" + name,
			ExpiresAt: time.Now().Add(time.Hour), Enabled: true, AuthStatus: accountdomain.AuthStatusActive,
			Priority: 200 - index, MaxConcurrent: 1,
		})
		if createErr != nil {
			t.Fatal(createErr)
		}
		credentials = append(credentials, credential)
	}
	if err := modelRepo.UpsertDiscovered(ctx, accountdomain.ProviderBuild, []string{"grok-4.6"}); err != nil {
		t.Fatal(err)
	}
	for _, credential := range credentials {
		if err := modelRepo.ReplaceAccountCapabilities(ctx, credential.ID, []string{"grok-4.6"}, time.Now().UTC()); err != nil {
			t.Fatal(err)
		}
	}
	clientKey, err := keyRepo.Create(ctx, clientkey.Key{
		Name: "context-guard-key", Prefix: "dqctx", SecretHash: strings.Repeat("b", 64), EncryptedSecret: "encrypted",
		Enabled: true, RPMLimit: 120, MaxConcurrent: 8,
	})
	if err != nil {
		t.Fatal(err)
	}
	// 200k 输入的真实形态：上游正常返回，只是这几个响应没有推理（降智命中）。
	bigContext := sse(
		`data: {"choices":[{"delta":{"content":"`+strings.Repeat("abcd", 40)+`"}}]}`,
		`data: {"usage":{"prompt_tokens":204675,"completion_tokens":40,"completion_tokens_details":{"reasoning_tokens":0}}}`,
		"data: [DONE]",
	)
	responses := make(map[uint64][]scriptedBuildResponse, len(credentials))
	for _, credential := range credentials {
		responses[credential.ID] = []scriptedBuildResponse{{status: http.StatusOK, body: bigContext}}
	}
	adapter := &scriptedBuildAdapter{responses: responses}
	registry := provider.NewRegistry(adapter)
	sticky := memory.NewStickyStore()
	accountService := accountapp.NewService(accountRepo, auditRepo, memory.NewDeviceSessionStore(), sticky, registry, testCipher(t), nil)
	selector := NewSelector(accountRepo, memory.NewConcurrencyLimiter(), sticky, registry, time.Hour, time.Second, time.Minute)
	service := NewService(modelRepo, auditRepo, accountService, clientkeyapp.NewService(nil, nil, nil, 60, 4, nil), registry, selector, responseRepo, 3)
	runtime := degradeTestConfig()
	runtime.Enabled = true
	runtime.MaxAttempts = 3
	runtime.MinOutputTokens = 32
	runtime.HoldTimeout = time.Second
	service.UpdateQualityRetry(runtime)

	result, err := service.CreateChatCompletion(ctx, Input{
		RequestID: "req-context-guard", ClientKey: clientKey, PublicModel: "grok-4.6", Streaming: true,
		Body: []byte(`{"model":"grok-4.6","messages":[{"role":"user","content":"summarize the whole repo"}],"stream":true}`),
	})
	if result != nil {
		t.Fatalf("degraded large-context response must not be delivered as 200, got status %d", result.StatusCode)
	}
	var upstreamFailure *UpstreamFailure
	if !errors.As(err, &upstreamFailure) {
		t.Fatalf("expected UpstreamFailure, got %v", err)
	}
	if upstreamFailure.Code == "context_window_exceeded" {
		t.Fatalf("large context must never abort with context_window_exceeded, got %d/%s", upstreamFailure.HTTPStatus, upstreamFailure.Code)
	}
	if upstreamFailure.HTTPStatus != http.StatusServiceUnavailable || upstreamFailure.Code != ErrorQualityDegraded {
		t.Fatalf("failure = %d/%s, want 503/%s", upstreamFailure.HTTPStatus, upstreamFailure.Code, ErrorQualityDegraded)
	}
	// 两个账号都应被尝试：大上下文不跳过重试。
	if attempts := adapter.Attempts(); len(attempts) != 2 {
		t.Fatalf("large-context degraded request must retry on another account, attempts=%#v", attempts)
	}
	// 大上下文标签命中现在照常进入归因统计（不再按输入大小丢弃样本），
	// 但每个账号只有 1 个样本，最多是 UNKNOWN 的短冷却，绝不能被禁用。
	for _, credential := range credentials {
		account, err := accountRepo.Get(ctx, credential.ID)
		if err != nil {
			t.Fatal(err)
		}
		if !account.Enabled {
			t.Fatalf("large-context hits must never disable accounts: %#v", account)
		}
		if account.CooldownUntil != nil {
			if remaining := time.Until(*account.CooldownUntil); remaining > 3*time.Minute {
				t.Fatalf("single-sample attribution must only apply short cooldown, got %#v", account)
			}
		}
	}
}
