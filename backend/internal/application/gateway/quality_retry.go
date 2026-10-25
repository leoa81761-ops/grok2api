package gateway

import (
	"context"
	"encoding/json"
	"errors"
	"math"
	"net/http"
	"strings"
	"time"

	accountdomain "github.com/chenyme/grok2api/backend/internal/domain/account"
	"github.com/chenyme/grok2api/backend/internal/domain/audit"
	inferencedomain "github.com/chenyme/grok2api/backend/internal/domain/inference"
	modeldomain "github.com/chenyme/grok2api/backend/internal/domain/model"
	infraegress "github.com/chenyme/grok2api/backend/internal/infra/egress"
	neterrorpkg "github.com/chenyme/grok2api/backend/internal/pkg/neterror"
)

const (
	ErrorQualityDegraded = "quality_degraded"
	// ErrorQualityDegradedNoAccount 表示归因确认降智、但重试期间候选账号已全部
	// 进入冷却。仍然返回 503，绝不用劣质响应冒充成功。
	ErrorQualityDegradedNoAccount          = "quality_degraded_no_account"
	qualityRetryFailOpen                   = "fail_open"
	qualityRetryFailClosed                 = "fail_closed"
	defaultQualityMaxAttempts              = 6
	defaultQualityHoldTimeout              = 30 * time.Second
	defaultQualityMinOutput                = int64(8)
	// 大上下文「空转收尾」窄规则：请求声明了 tools、上游没有工具调用、没有推理，
	// 且上下文很大时，可见输出低于阈值即视为空回复。线上实测：输入 15–18 万
	// token 的请求只回 33–63 token、reasoning=0，却被当成 200 成功交付，客户端
	// 显示「已完成」但没有答案。
	defaultAgentMinInputTokens  = int64(100_000)
	defaultAgentMinOutputTokens = int64(64)
	defaultMinEncryptedBytes               = 256
	defaultEncryptedBytesPerReasoningToken = 4
	defaultBurstFlushMS                    = int64(1000)
	defaultBurstMaxVisible                 = int64(32)
	defaultBurstMinReasoning               = int64(80)
	// Fake encrypted thinking dumps the whole visible answer after a long
	// wait. Audit TPS is rewritten against full duration (looks like 60–120
	// tok/s) but first-token ≈ duration. Catch flush windows up to 2s so
	// 1.8s / 1962-token dumps are withheld too.
	defaultFakeEncFlushMS = int64(2000)
	// Cipher-only "thinking" that is already dumping this much visible text
	// with usage.reasoning_tokens=0 is the 128k status-loop drool, not a
	// real encrypted thinking stream.
	defaultCipherDroolVisible        = int64(1024)
	defaultMissingThinkingCooldown   = 12 * time.Hour
	lastErrorMissingThinking         = accountdomain.LastErrorMissingThinking
	lastErrorMissingThinkingDisabled = accountdomain.LastErrorMissingThinkingDisabled
	// An empty stream that idles while held is treated as an account-quality
	// failure: the request can still rotate before any bytes reach the client.
	qualityIdleAccountCooldown = 15 * time.Minute
)

var (
	errQualityDegraded    = errors.New("上游响应缺少推理")
	errQualityEmptyStream = errors.New("上游流式响应为空")
)

// QualityRetryRuntime is the isolated request-path withhold/retry policy.
// Zero Enabled leaves production behavior unchanged.
type QualityRetryRuntime struct {
	Enabled         bool
	MaxAttempts     int
	HoldTimeout     time.Duration
	MinOutputTokens int64
	OnExhausted     string
	AutoDisable     bool
	AccountCooldown time.Duration
	// IdleAccountCooldown is applied to truly empty upstream streams
	// (idle timeout / empty peek). Missing-thinking still uses AccountCooldown.
	IdleAccountCooldown             time.Duration
	MinEncryptedBytes               int
	EncryptedBytesPerReasoningToken int
	SoftTPS                         float64
	HardTPS                         float64
	MinGenerationMS                 int64
	FailClosed                      bool
	// AgentMinInputTokens / AgentMinOutputTokens 控制「大上下文空转收尾」窄规则：
	// 输入 ≥ AgentMinInputTokens 且可见输出 < AgentMinOutputTokens、声明了 tools
	// 但没有工具调用与推理时，判定为降智空回复并换号重试。
	AgentMinInputTokens  int64
	AgentMinOutputTokens int64
	// AbortHoldTimeoutNoOutput 打开后，持流到期且上游完全没有可见输出时立即
	// 按空流失败（换号重试/快速 504），避免下游空闲到被客户端掐断。
	AbortHoldTimeoutNoOutput bool
	// DegradeAttributionEnabled 打开降智归因与分类处置。零值保持历史行为
	// （每次 Withhold 无条件冷却/禁用账号），生产配置默认开启。
	DegradeAttributionEnabled bool
	// DegradationMode 是灰度开关：observe（默认）只算归因/写日志、不产生处置
	// 副作用；enforce 才隔离租约/冷却禁用账号/关节点调度。
	DegradationMode         string
	DegradeWindow           time.Duration
	DegradeMinSamples       int
	DegradeAccountThreshold float64
	DegradeNodeThreshold    float64
	DegradeNodeMinAccounts  int
	// DegradeMaxAttributionInputTokens 是计入归因统计的输入 token 上限；
	// 超过它的命中只打 context_large 观测标签，不参与账号/节点统计（线上
	// 1 天数据里 input≈130k 的请求 92% 无推理是相关而非因果，成因待定位）。
	DegradeMaxAttributionInputTokens int64
	// DegradeShapeMinGenerationMS 以下的生成窗口视为缓冲倾倒/测量伪影（SHAPE），
	// 只跳过一次归因统计，不惩罚账号或节点。
	DegradeShapeMinGenerationMS int64
	DegradeAccountCooldowns     []time.Duration
	DegradeMaxStrikes           int
	// DegradeQuarantineDuration 是账号×出口节点租约隔离（首选处置）的时长。
	DegradeQuarantineDuration time.Duration
	DegradeUnknownCooldown    time.Duration
	// DegradeGlobalSuppressThreshold 是全局退化率抑制线：全站普遍退化时不再
	// 逐账号/节点处置，避免把上游整体故障误判成大量坏账号。
	DegradeGlobalSuppressThreshold float64
	// DegradeEmptyToolTurn 拦截「声明了 tools 却没有工具调用、没有推理、
	// 可见与总输出都低于 MinOutputTokens」的空转收尾。必须保持窄。
	DegradeEmptyToolTurn bool
}

// QualityStreamSignals is the hold classifier input. Tests drive this
// directly and via ObserveQualityChunk on SSE fixtures.
type QualityStreamSignals struct {
	HasThinking       bool
	HasReasoningDelta bool
	// ReasoningStarted is an empty reasoning item or the Chat SSE stub
	// `: grok2api-reasoning-start`. That is not proof of thinking: 降智
	// still emits the stub, then dumps visible tokens with usage 0.
	ReasoningStarted bool
	VisibleTokens    int64
	ReasoningTokens  int64
	OutputTokens     int64
	EncryptedBytes   int
	EncryptedFloor   int64
	UsageReported    bool
	FirstVisible     bool
	VisibleFlushMS   int64
	Terminal         bool
	HoldExpired      bool
	// SemanticOutput 表示流里出现过工具调用、函数调用或非文本输出项。
	SemanticOutput bool
	// ToolsDeclared 表示请求体声明了非空 tools；DegradeEmptyToolTurn 是策略开关。
	ToolsDeclared        bool
	DegradeEmptyToolTurn bool
	// InputTokens 是上游回报的输入 token（上下文规模）。
	InputTokens int64
	// AgentMinInputTokens / AgentMinOutputTokens 是「大上下文空转收尾」窄规则的
	// 阈值，由 runtime 配置注入；零值表示使用内置默认。
	AgentMinInputTokens  int64
	AgentMinOutputTokens int64
}

// QualityVerdict is the hold decision for one upstream stream.
type QualityVerdict string

const (
	QualityWait     QualityVerdict = "wait"
	QualityDeliver  QualityVerdict = "deliver"
	QualityWithhold QualityVerdict = "withhold"
)

// QualityRetryAction is what the attempt loop does with a withhold verdict.
type QualityRetryAction string

const (
	QualityActionDeliver     QualityRetryAction = "deliver"
	QualityActionDeliverLast QualityRetryAction = "deliver_last"
	QualityActionRetry       QualityRetryAction = "retry"
	QualityActionReject      QualityRetryAction = "reject"
)

func normalizeQualityRetry(cfg QualityRetryRuntime) QualityRetryRuntime {
	if cfg.MaxAttempts <= 0 {
		cfg.MaxAttempts = defaultQualityMaxAttempts
	}
	if cfg.HoldTimeout <= 0 {
		cfg.HoldTimeout = defaultQualityHoldTimeout
	}
	if cfg.MinOutputTokens <= 0 {
		cfg.MinOutputTokens = defaultQualityMinOutput
	}
	if cfg.AccountCooldown <= 0 {
		cfg.AccountCooldown = defaultMissingThinkingCooldown
	}
	if cfg.IdleAccountCooldown <= 0 {
		cfg.IdleAccountCooldown = qualityIdleAccountCooldown
	}
	if cfg.MinEncryptedBytes <= 0 {
		cfg.MinEncryptedBytes = defaultMinEncryptedBytes
	}
	if cfg.EncryptedBytesPerReasoningToken <= 0 {
		cfg.EncryptedBytesPerReasoningToken = defaultEncryptedBytesPerReasoningToken
	}
	if cfg.SoftTPS <= 0 {
		cfg.SoftTPS = audit.DefaultDegradeSoftTPS
	}
	if cfg.HardTPS <= cfg.SoftTPS {
		cfg.HardTPS = audit.DefaultDegradeHardTPS
	}
	if cfg.MinGenerationMS <= 0 {
		cfg.MinGenerationMS = audit.DefaultDegradeMinGenMS
	}
	if cfg.AgentMinInputTokens <= 0 {
		cfg.AgentMinInputTokens = defaultAgentMinInputTokens
	}
	if cfg.AgentMinOutputTokens <= 0 {
		cfg.AgentMinOutputTokens = defaultAgentMinOutputTokens
	}
	cfg.DegradationMode = normalizeDegradationMode(cfg.DegradationMode)
	cfg.DegradeWindow = normalizeDegradeWindow(cfg.DegradeWindow)
	cfg.DegradeMinSamples = normalizeDegradeMinSamples(cfg.DegradeMinSamples)
	cfg.DegradeAccountThreshold = normalizeDegradeThreshold(cfg.DegradeAccountThreshold, defaultDegradeAccountThreshold)
	cfg.DegradeNodeThreshold = normalizeDegradeThreshold(cfg.DegradeNodeThreshold, defaultDegradeNodeThreshold)
	cfg.DegradeNodeMinAccounts = normalizeDegradeNodeMinAccounts(cfg.DegradeNodeMinAccounts)
	cfg.DegradeMaxAttributionInputTokens = normalizeDegradeTokenLimit(cfg.DegradeMaxAttributionInputTokens, defaultDegradeMaxAttributionInputTokens)
	cfg.DegradeShapeMinGenerationMS = normalizeDegradeGenWindowMS(cfg.DegradeShapeMinGenerationMS)
	cfg.DegradeAccountCooldowns = normalizeDegradeLadder(cfg.DegradeAccountCooldowns)
	cfg.DegradeMaxStrikes = normalizeDegradeMaxStrikes(cfg.DegradeMaxStrikes)
	cfg.DegradeQuarantineDuration = normalizeDegradeCooldown(cfg.DegradeQuarantineDuration, defaultDegradeQuarantineDuration)
	cfg.DegradeUnknownCooldown = normalizeDegradeCooldown(cfg.DegradeUnknownCooldown, defaultDegradeUnknownCooldown)
	cfg.DegradeGlobalSuppressThreshold = normalizeDegradeSuppressThreshold(cfg.DegradeGlobalSuppressThreshold)
	cfg.OnExhausted = normalizeQualityExhaustionPolicy(cfg.OnExhausted)
	return cfg
}

func (s *Service) UpdateQualityRetry(cfg QualityRetryRuntime) {
	normalized := normalizeQualityRetry(cfg)
	s.qualityRetry.Store(&normalized)
}

func (s *Service) qualityRetryConfig() QualityRetryRuntime {
	if s == nil {
		return normalizeQualityRetry(QualityRetryRuntime{})
	}
	if value := s.qualityRetry.Load(); value != nil {
		return *value
	}
	return normalizeQualityRetry(QualityRetryRuntime{})
}

// encryptedThinkingFloor is max(minBytes, reasoningTokens*bytesPerToken).
// A non-empty stub such as "gAAAA-cipher" is not thinking.
func encryptedThinkingFloor(minBytes, bytesPerToken int, reasoningTokens int64) int64 {
	if minBytes <= 0 {
		minBytes = defaultMinEncryptedBytes
	}
	if bytesPerToken <= 0 {
		bytesPerToken = defaultEncryptedBytesPerReasoningToken
	}
	floor := int64(minBytes)
	if reasoningTokens > 0 {
		if reasoningTokens > math.MaxInt64/int64(bytesPerToken) {
			return math.MaxInt64
		}
		need := reasoningTokens * int64(bytesPerToken)
		if need > floor {
			floor = need
		}
	}
	return floor
}

func qualityFastFlush(sig QualityStreamSignals, limitMS int64) bool {
	return sig.FirstVisible && sig.VisibleFlushMS >= 0 && sig.VisibleFlushMS < limitMS
}

func qualityMeetsEncryptedFloor(sig QualityStreamSignals) bool {
	if sig.EncryptedBytes <= 0 {
		return false
	}
	floor := sig.EncryptedFloor
	if floor <= 0 {
		floor = encryptedThinkingFloor(0, 0, sig.ReasoningTokens)
	}
	return int64(sig.EncryptedBytes) >= floor
}

func qualityHasDumpBill(sig QualityStreamSignals) bool {
	return sig.ReasoningTokens >= defaultBurstMinReasoning || qualityMeetsEncryptedFloor(sig)
}

func qualityIsBurstDump(sig QualityStreamSignals, minOutput int64) bool {
	_ = minOutput
	if sig.HasReasoningDelta {
		return false
	}
	visible := sig.VisibleTokens
	heavyReasoning := sig.ReasoningTokens >= defaultBurstMinReasoning
	shortVisible := visible > 0 && visible < defaultBurstMaxVisible
	// Hold timed out, then a short greeting dumped with a large reasoning bill
	// (TUI "你好" after 30s / 954 thinking tokens).
	if sig.HoldExpired && shortVisible && heavyReasoning {
		return true
	}
	if qualityFastFlush(sig, defaultBurstFlushMS) && qualityHasDumpBill(sig) {
		return true
	}
	return false
}

// qualityIsFakeEncryptedDump is the 18190 / 18183 dump: ciphertext or a
// large reasoning bill, then the visible answer arrives in <2s. Visible
// token count is not a gate — vis<8 chat dumps were leaking on minOutput.
func qualityIsFakeEncryptedDump(sig QualityStreamSignals, minOutput int64) bool {
	_ = minOutput
	if sig.HasReasoningDelta {
		return false
	}
	if !qualityFastFlush(sig, defaultFakeEncFlushMS) {
		return false
	}
	return qualityHasDumpBill(sig)
}

// qualityIsFastReasoningRatioDump catches plaintext thinking that is still a
// 1ms dump: billed reasoning is ≥80% of output and the visible flush is <2s.
func qualityIsFastReasoningRatioDump(sig QualityStreamSignals) bool {
	if !sig.HasReasoningDelta {
		return false
	}
	if !qualityFastFlush(sig, defaultFakeEncFlushMS) {
		return false
	}
	output := sig.OutputTokens
	if output <= 0 {
		output = sig.VisibleTokens + sig.ReasoningTokens
	}
	if output <= 0 || sig.ReasoningTokens <= 0 {
		return false
	}
	return sig.ReasoningTokens*5 >= output*4
}

// qualityIsCipherDrool is the 128k TUI status-loop: ciphertext met the
// floor so HasThinking is true, but there is no plaintext reasoning and
// usage.reasoning_tokens is still 0 while visible text is already dumping.
func qualityIsCipherDrool(sig QualityStreamSignals, minOutput int64) bool {
	if minOutput <= 0 {
		minOutput = defaultQualityMinOutput
	}
	if sig.HasReasoningDelta || sig.ReasoningTokens > 0 {
		return false
	}
	if sig.EncryptedBytes <= 0 {
		return false
	}
	visible := sig.VisibleTokens
	if visible >= defaultCipherDroolVisible {
		return true
	}
	if sig.Terminal && visible >= minOutput {
		return true
	}
	return false
}

// qualityIsEmptyToolTurn catches an agent turn that declared tools but ended
// with a few-token plain-text response: no tool call, no reasoning, and both
// the visible and total output stay under minOutput. Client agents close the
// whole task on such a reply, so it cannot be a valid finish. The rule is
// deliberately narrow: a normal text finish with >= minOutput tokens must stay
// deliverable, otherwise every short completion would be withheld and retried
// into a 503 storm on pools where most responses carry no reasoning tokens.
func qualityIsEmptyToolTurn(sig QualityStreamSignals, minOutput int64) bool {
	if minOutput <= 0 {
		minOutput = defaultQualityMinOutput
	}
	if !sig.DegradeEmptyToolTurn || !sig.ToolsDeclared || !sig.Terminal || sig.SemanticOutput {
		return false
	}
	if sig.HasThinking || sig.ReasoningTokens > 0 {
		return false
	}
	return sig.VisibleTokens < minOutput && sig.OutputTokens < minOutput
}

// qualityIsEmptyAgentContext 是「大上下文空转收尾」窄规则：
//   - 请求声明了 tools（agent 回合），但流里没有出现任何工具调用/函数调用；
//   - 没有任何推理证据；
//   - 上下文规模 ≥ AgentMinInputTokens（默认 10 万 token）；
//   - 可见输出 < AgentMinOutputTokens（默认 64 token）。
//
// 线上触发形态：输入 15–18 万 token、输出 33–63 token、reasoning=0，被当成
// 200 成功交付，客户端显示「已完成」但正文为空。该规则只用于扣留换号，
// 不改变普通短回复（上下文小或带工具调用）的交付。
func qualityIsEmptyAgentContext(sig QualityStreamSignals) bool {
	if !sig.ToolsDeclared || !sig.Terminal || sig.SemanticOutput {
		return false
	}
	if sig.HasThinking || sig.HasReasoningDelta || sig.ReasoningTokens > 0 {
		return false
	}
	minInput := sig.AgentMinInputTokens
	if minInput <= 0 {
		minInput = defaultAgentMinInputTokens
	}
	minOutput := sig.AgentMinOutputTokens
	if minOutput <= 0 {
		minOutput = defaultAgentMinOutputTokens
	}
	if sig.InputTokens < minInput {
		return false
	}
	return sig.VisibleTokens < minOutput
}

// qualityWithholdRule 记录这次 Withhold 由哪条判定规则触发。降智归因用它区分
// 「标签命中」（缺思考/截断，属于网关自身结论，一律计入统计）与「空转窄规则」
// （形状命中，才需要按 gen_window/input 分层剔除测量伪影）。
type qualityWithholdRule string

const (
	qualityWithholdRuleNone            qualityWithholdRule = ""
	qualityWithholdRuleBurstDump       qualityWithholdRule = "burst_dump"
	qualityWithholdRuleCipherDrool     qualityWithholdRule = "cipher_drool"
	qualityWithholdRuleFakeEncrypted   qualityWithholdRule = "fake_encrypted_dump"
	qualityWithholdRuleFastReasoning   qualityWithholdRule = "fast_reasoning_ratio"
	qualityWithholdRuleMissingThinking qualityWithholdRule = "missing_thinking"
	qualityWithholdRuleEmptyToolTurn   qualityWithholdRule = "empty_tool_turn"
	// 大上下文空转收尾：输入很大、声明了 tools、没有工具调用与推理、可见输出极低。
	qualityWithholdRuleEmptyAgentContext qualityWithholdRule = "empty_agent_context"
)

// ClassifyQualityHold decides whether a held stream may be forwarded.
// Dump detectors run first so plaintext thinking cannot veto a 1ms
// reasoning-ratio dump, and vis<minOutput cannot skip fake-enc/burst.
// Remaining plaintext deltas still deliver. Cipher-only HasThinking waits
// until visible text has streamed for 2s or the stream ends.
func ClassifyQualityHold(sig QualityStreamSignals, minOutput int64) QualityVerdict {
	verdict, _ := classifyQualityHold(sig, minOutput)
	return verdict
}

// classifyQualityHold 是 ClassifyQualityHold 的带触发规则版本，供降智归因区分
// 标签命中与空转窄规则形状命中。
func classifyQualityHold(sig QualityStreamSignals, minOutput int64) (QualityVerdict, qualityWithholdRule) {
	if minOutput <= 0 {
		minOutput = defaultQualityMinOutput
	}
	switch {
	case qualityIsBurstDump(sig, minOutput):
		return QualityWithhold, qualityWithholdRuleBurstDump
	case qualityIsCipherDrool(sig, minOutput):
		return QualityWithhold, qualityWithholdRuleCipherDrool
	case qualityIsFakeEncryptedDump(sig, minOutput):
		return QualityWithhold, qualityWithholdRuleFakeEncrypted
	case qualityIsFastReasoningRatioDump(sig):
		return QualityWithhold, qualityWithholdRuleFastReasoning
	case qualityIsEmptyToolTurn(sig, minOutput):
		return QualityWithhold, qualityWithholdRuleEmptyToolTurn
	case qualityIsEmptyAgentContext(sig):
		return QualityWithhold, qualityWithholdRuleEmptyAgentContext
	}
	if sig.HasThinking {
		if sig.HasReasoningDelta {
			return QualityDeliver, qualityWithholdRuleNone
		}
		// Cipher-only: do not release when encrypted_content first meets
		// the floor. Fake dumps send the blob, then the whole answer in
		// <2s; releasing early lets that dump bypass fake-enc. Wait until
		// visible text has streamed for 2s, or the stream ends.
		if sig.Terminal {
			return QualityDeliver, qualityWithholdRuleNone
		}
		if sig.VisibleTokens >= minOutput && sig.FirstVisible && sig.VisibleFlushMS >= defaultFakeEncFlushMS {
			return QualityDeliver, qualityWithholdRuleNone
		}
		return QualityWait, qualityWithholdRuleNone
	}
	// Prefer observed/derived visible output. Total output includes reasoning
	// tokens, which are deliberately not trusted as quality evidence above. If
	// the stream exposed no visible count at all, retain OutputTokens as a
	// compatibility fallback for terminal usage-only responses.
	output := sig.VisibleTokens
	if output <= 0 {
		output = sig.OutputTokens
	}
	enough := output >= minOutput
	// 声明 tools 的 agent 回合按「工具调用 + 输出量」判定：线上 63%–99% 的
	// Build 响应本来就没有 reasoning tokens，缺推理不能当降智证据（窄空转规则
	// 已在上面处理）。否则几乎每个正常纯文本收尾都会被扣留并重试成 503。
	if sig.ToolsDeclared && sig.DegradeEmptyToolTurn {
		if enough || sig.Terminal {
			return QualityDeliver, qualityWithholdRuleNone
		}
		return QualityWait, qualityWithholdRuleNone
	}
	if sig.ReasoningStarted && !sig.Terminal && !sig.HoldExpired {
		return QualityWait, qualityWithholdRuleNone
	}
	if sig.Terminal {
		if output <= 0 {
			return QualityWait, qualityWithholdRuleNone
		}
		if enough {
			return QualityWithhold, qualityWithholdRuleMissingThinking
		}
		return QualityDeliver, qualityWithholdRuleNone
	}
	if enough {
		return QualityWithhold, qualityWithholdRuleMissingThinking
	}
	if sig.HoldExpired {
		if output <= 0 {
			return QualityWait, qualityWithholdRuleNone
		}
		if enough {
			return QualityWithhold, qualityWithholdRuleMissingThinking
		}
		return QualityDeliver, qualityWithholdRuleNone
	}
	return QualityWait, qualityWithholdRuleNone
}

// qualityPeekAbortError prefers the idle-timeout cause over a plain
// context.Canceled so the attempt loop can retry instead of treating the
// abort as a client 499.
func qualityPeekAbortError(ctx context.Context, err error) error {
	if ctx != nil {
		if cause := context.Cause(ctx); neterrorpkg.IsUpstreamStreamIdleTimeout(cause) {
			return cause
		}
	}
	if neterrorpkg.IsUpstreamStreamIdleTimeout(err) {
		return err
	}
	if err != nil {
		return err
	}
	if ctx != nil {
		return ctx.Err()
	}
	return nil
}

// isClientRequestCancel reports a real client disconnect. Upstream idle
// timeouts cancel the same context and must not be classified as 499.
func isClientRequestCancel(ctx context.Context, err error) bool {
	return neterrorpkg.IsClientRequestCancel(ctx, err)
}

// DecideQualityRetry caps withhold recovery at maxAttempts (default 6:
// original + five extra accounts). The last withhold
// (attemptIndex == maxAttempts-1) is fail-open unless OnExhausted is fail_closed.
func DecideQualityRetry(verdict QualityVerdict, attemptIndex, maxAttempts int, onExhausted string) QualityRetryAction {
	if verdict != QualityWithhold {
		return QualityActionDeliver
	}
	if maxAttempts <= 0 {
		maxAttempts = defaultQualityMaxAttempts
	}
	if attemptIndex < 0 {
		attemptIndex = 0
	}
	if attemptIndex < maxAttempts-1 {
		return QualityActionRetry
	}
	// attemptIndex == maxAttempts-1 (or past it): do not retry again.
	if normalizeQualityExhaustionPolicy(onExhausted) == qualityRetryFailClosed {
		return QualityActionReject
	}
	return QualityActionDeliverLast
}

// BoundQualityRetry turns a Retry into DeliverLast/Reject when the routing
// loop has no remaining account slot, so the already-held body is not dropped
// on continue-into-exhausted-loop.
func BoundQualityRetry(action QualityRetryAction, hasNextRoutingAttempt bool, onExhausted string) QualityRetryAction {
	if action != QualityActionRetry || hasNextRoutingAttempt {
		return action
	}
	if normalizeQualityExhaustionPolicy(onExhausted) == qualityRetryFailClosed {
		return QualityActionReject
	}
	return QualityActionDeliverLast
}

func normalizeQualityExhaustionPolicy(value string) string {
	if strings.EqualFold(strings.TrimSpace(value), qualityRetryFailOpen) {
		return qualityRetryFailOpen
	}
	return qualityRetryFailClosed
}

// QualityCommit is the single attempt-loop decision for a held stream.
type QualityCommit struct {
	Action   QualityRetryAction
	Audit    bool
	KeepBody bool
}

// CommitQualityHold is the shipped withhold/retry/commit unit. The attempt
// loop must not re-derive this from Decide+Bound+switch.
func CommitQualityHold(verdict QualityVerdict, qualityAttempt, maxAttempts int, hasNextRouting bool, onExhausted string) QualityCommit {
	action := BoundQualityRetry(
		DecideQualityRetry(verdict, qualityAttempt, maxAttempts, onExhausted),
		hasNextRouting,
		onExhausted,
	)
	switch action {
	case QualityActionRetry, QualityActionReject:
		return QualityCommit{Action: action, Audit: true, KeepBody: false}
	case QualityActionDeliverLast:
		return QualityCommit{Action: action, Audit: false, KeepBody: true}
	default:
		return QualityCommit{Action: QualityActionDeliver, Audit: false, KeepBody: true}
	}
}

func shouldHoldQualityStream(input Input, ownership *inferencedomain.ResponseOwnership, route modeldomain.Route, operation audit.Operation, cfg QualityRetryRuntime) bool {
	if !cfg.Enabled || !input.Streaming || input.ForcedEgressNodeID != 0 || input.skipQualityHold {
		return false
	}
	switch operation {
	case audit.OperationChat, audit.OperationResponses, audit.OperationMessages, "":
	default:
		return false
	}
	// Context compaction is a system summary operation, not a normal reasoning
	// turn. Holding it can quarantine a healthy account for producing the
	// expected summary without streamed reasoning. Keep both compaction forms
	// excluded even if a caller reaches this gate without skipQualityHold.
	if isResponsesCompactionRequest(input.Body) {
		return false
	}
	if route.Provider != accountdomain.ProviderBuild && route.Provider != accountdomain.ProviderConsole {
		return false
	}
	// TUI commonly declares tools and follow-ups carry previous_response_id.
	// They still need quality classification, but replay safety is decided
	// separately: detecting a degraded response must not imply that an
	// account-bound or side-effecting request can run on another account.
	if qualityRequestDisablesReasoning(input.Body) {
		return false
	}
	if modeldomain.SupportsReasoningForProvider(route.Provider, input.PublicModel) {
		return true
	}
	return modeldomain.SupportsReasoningForProvider(route.Provider, route.UpstreamModel)
}

// canReplayQualityHoldAcrossAccounts separates response classification from
// retry authority. Stored Responses are account-bound, while hosted tools may
// already have produced an external side effect before their held response is
// rejected. Both may be held, audited, and penalized, but neither is replayed
// on another account.
func canReplayQualityHoldAcrossAccounts(input Input, ownership *inferencedomain.ResponseOwnership) bool {
	return ownership == nil && !qualityRequestHasReplayUnsafeHostedTools(input.Body)
}

func qualityRequestHasReplayUnsafeHostedTools(body []byte) bool {
	var payload map[string]any
	if json.Unmarshal(body, &payload) != nil || payload == nil {
		return false
	}
	if raw, exists := payload["web_search_options"]; exists && raw != nil {
		return true
	}
	if raw, exists := payload["mcp_servers"]; exists && raw != nil {
		servers, ok := raw.([]any)
		if !ok || len(servers) > 0 {
			return true
		}
	}
	if qualityToolListHasReplayUnsafeHostedTool(payload["tools"]) {
		return true
	}
	// Responses Tool Search can load declarations later in the request. Only
	// inspect additional_tools items; arbitrary user/schema objects may also
	// contain a field named "tools" and must not affect the retry policy.
	items, _ := payload["input"].([]any)
	for _, rawItem := range items {
		item, ok := rawItem.(map[string]any)
		if !ok || jsonNodeString(item["type"]) != "additional_tools" {
			continue
		}
		if qualityToolListHasReplayUnsafeHostedTool(item["tools"]) {
			return true
		}
	}
	return false
}

func qualityToolListHasReplayUnsafeHostedTool(value any) bool {
	tools, ok := value.([]any)
	if !ok {
		return false
	}
	for _, rawTool := range tools {
		tool, ok := rawTool.(map[string]any)
		if !ok {
			continue
		}
		kind := jsonNodeString(tool["type"])
		switch kind {
		case "", "function", "custom", "local_shell", "apply_patch", "tool_search":
			// These declarations only ask the model to return a call. Execution
			// happens in the client after the held response is committed.
			continue
		case "shell":
			environment, _ := tool["environment"].(map[string]any)
			if jsonNodeString(environment["type"]) != "local" {
				return true
			}
		case "namespace":
			if qualityToolListHasReplayUnsafeHostedTool(tool["tools"]) {
				return true
			}
		default:
			// Default to no replay for every server/native tool, including types
			// added by future protocol versions that this gateway does not know yet.
			return true
		}
	}
	return false
}

func jsonNodeString(value any) string {
	text, _ := value.(string)
	return strings.ToLower(strings.TrimSpace(text))
}

func qualityRequestDisablesReasoning(body []byte) bool {
	var payload map[string]json.RawMessage
	if json.Unmarshal(body, &payload) != nil {
		return false
	}
	if jsonStringEquals(payload["reasoning_effort"], modeldomain.ReasoningEffortNone) {
		return true
	}
	for _, key := range []string{"reasoning", "output_config", "thinking"} {
		var nested map[string]json.RawMessage
		if json.Unmarshal(payload[key], &nested) != nil {
			continue
		}
		if jsonStringEquals(nested["effort"], modeldomain.ReasoningEffortNone) || jsonStringEquals(nested["type"], "disabled") {
			return true
		}
		var budget int64
		if raw, ok := nested["budget_tokens"]; ok && json.Unmarshal(raw, &budget) == nil && budget == 0 {
			return true
		}
	}
	return jsonStringEquals(payload["thinking"], "disabled")
}

func jsonStringEquals(raw json.RawMessage, want string) bool {
	var value string
	return json.Unmarshal(raw, &value) == nil && strings.EqualFold(strings.TrimSpace(value), want)
}

func (s *Service) applyMissingThinkingPenalty(ctx context.Context, requestID string, credential accountdomain.Credential, cooldown time.Duration, autoDisable bool) {
	writeCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), finalizationTimeout)
	defer cancel()
	action, err := s.selector.markMissingThinking(writeCtx, credential, cooldown, autoDisable)
	if err != nil {
		s.logger.Error("quality_degraded_penalty_failed", "request_id", requestID, "account_id", credential.ID, "action", action, "error", err)
		return
	}
	switch action {
	case missingThinkingPenaltyDisabled:
		s.logger.Info("quality_degraded_disabled", "request_id", requestID, "account_id", credential.ID)
	case missingThinkingPenaltyCooled:
		s.logger.Info("quality_degraded_cooldown", "request_id", requestID, "account_id", credential.ID, "cooldown", cooldown.String())
	}
}

func (s *Service) applyOutputSpeedPenalty(ctx context.Context, record audit.Record, credential accountdomain.Credential, cfg QualityRetryRuntime, forcedEgress bool) error {
	// Both explicit and TUI compaction are classified in the audit record.
	// System summaries stay exempt from account quality penalties.
	if record.Operation == audit.OperationCompaction {
		return nil
	}
	if !cfg.Enabled || !cfg.AutoDisable || forcedEgress || credential.Provider != accountdomain.ProviderBuild || !record.Streaming || record.FirstTokenMS == nil || record.OutputTokens < cfg.MinOutputTokens || !auditRequestSucceeded(record.StatusCode, record.ErrorCode) {
		return nil
	}
	class, tps, _ := audit.ClassifyOutputSpeed(record.OutputTokens, record.ReasoningTokens, *record.FirstTokenMS, record.DurationMS, cfg.SoftTPS, cfg.HardTPS, cfg.MinGenerationMS, cfg.FailClosed)
	if class == "" {
		return nil
	}
	if err := s.selector.disableAccount(ctx, credential); err != nil {
		return err
	}
	s.logger.Info("quality_degraded_disabled", "request_id", record.RequestID, "account_id", credential.ID, "class", class, "output_tps", tps)
	return nil
}

func (s *Service) recordQualityDegraded(ctx context.Context, base audit.Record, credential accountdomain.Credential, usage Usage, startedAt time.Time, trace *infraegress.Trace, provider accountdomain.Provider) {
	s.recordQualityFailure(ctx, base, credential, usage, startedAt, trace, provider, ErrorQualityDegraded)
}

// recordQualityFailure 写入一条质量失败审计记录，errorCode 用于区分
// quality_degraded / context_window_exceeded 等不同失败语义。
func (s *Service) recordQualityFailure(ctx context.Context, base audit.Record, credential accountdomain.Credential, usage Usage, startedAt time.Time, trace *infraegress.Trace, provider accountdomain.Provider, errorCode string) {
	record := base
	record.EventID = newAuditEventID()
	accountID := credential.ID
	record.AccountID = &accountID
	record.AccountName = credential.Name
	record.StatusCode = http.StatusOK
	record.ErrorCode = errorCode
	record.OutputTokens = usage.OutputTokens
	record.ReasoningTokens = usage.ReasoningTokens
	record.TotalTokens = usage.TotalTokens
	record.InputTokens = usage.InputTokens
	if usage.Reported {
		record.UsageSource = audit.UsageSourceUpstream
	}
	record.DurationMS = time.Since(startedAt).Milliseconds()
	record.CreatedAt = time.Now().UTC()
	applyAuditEgress(&record, trace, provider)
	writeCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), finalizationTimeout)
	defer cancel()
	if err := s.audits.Create(writeCtx, record); err != nil {
		s.logger.Error("quality_degraded_audit_failed", "event_id", record.EventID, "request_id", record.RequestID, "error", err)
	}
}
