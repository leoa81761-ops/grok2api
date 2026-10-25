package gateway

import (
	"context"
	"encoding/json"
	"errors"
	"strings"
	"sync"
	"time"

	accountdomain "github.com/chenyme/grok2api/backend/internal/domain/account"
	egressdomain "github.com/chenyme/grok2api/backend/internal/domain/egress"
	infraegress "github.com/chenyme/grok2api/backend/internal/infra/egress"
	"github.com/chenyme/grok2api/backend/internal/infra/security"
	"github.com/chenyme/grok2api/backend/internal/repository"
)

// 降智归因默认值。线上基线（ops47/degrade-attribution-data.md）证明：
//   - direct 出口 99%、proxy 节点 63%–99% 的 audit reasoning_tokens=0，
//     所以 reasoning=0 本身不是降智证据，必须用网关自身的 Withhold 判定与
//     上游流式截断作为退化事件；
//   - 直接用「reason0 & out>=32」当命中会让 12 个被测账号里 8 个在 24h 内被
//     0.6 阈值杀掉（H0 期望误杀 10/天），因此命中还要按输入 token 与生成窗口
//     分层：大上下文只打 CONTEXT 观测标签（不参与处置决策），<500ms 生成窗口
//     是 SHAPE 测量伪影，两者都不进入账号/节点统计。
const (
	defaultDegradeWindow                    = 30 * time.Minute
	defaultDegradeMinSamples                = 5
	defaultDegradeAccountThreshold          = 0.6
	defaultDegradeNodeThreshold             = 0.8
	defaultDegradeNodeMinAccounts           = 2
	defaultDegradeMaxAttributionInputTokens = int64(50000)
	defaultDegradeShapeMinGenerationMS      = int64(500)
	defaultDegradeMaxStrikes                = 3
	defaultDegradeQuarantineDuration        = 15 * time.Minute
	defaultDegradeUnknownCooldown           = 2 * time.Minute
	defaultDegradeGlobalSuppressThreshold   = 0.9
	// degradeSamplesPerKey 是单个计数器的样本上限，避免长时间窗口内无界增长。
	degradeSamplesPerKey = 512
	// degradeGlobalSuppressMinSamplesFactor 要求全局抑制线至少有多倍 minSamples
	// 的全局样本，避免测试或小流量窗口里几条全退化样本就误判成全站故障。
	degradeGlobalSuppressMinSamplesFactor = 4
	// errorCodeUpstreamStreamIncomplete 是上游流式响应没有终止事件就 EOF 的审计码，
	// 与质量守卫 Withhold 一起归入 degradation 事件。
	errorCodeUpstreamStreamIncomplete = "upstream_stream_incomplete"
)

// 安全红线：NODE 处置绝不能写 egress 节点的 health/probe_status 字段。
// AutoRemoveUnhealthyNodes 每 60s 扫描一次，health<0.2（默认）会直接删除节点，
// 而线上每个节点绑定 215–216 个账号，删除节点会连带清空这些账号的绑定。
// 节点级处置只用调度开关（UpdateManyEnabled(false)，不碰健康度），
// 账号×节点精确处置只用 account_egress_lease_blocks 租约。
const degradationActionNone = "none"

// defaultDegradeAccountLadder 是账号冷却阶梯默认值：5m → 15m → 1h。
func defaultDegradeAccountLadder() []time.Duration {
	return []time.Duration{5 * time.Minute, 15 * time.Minute, time.Hour}
}

func normalizeDegradeWindow(value time.Duration) time.Duration {
	if value <= 0 {
		return defaultDegradeWindow
	}
	return value
}

func normalizeDegradeMinSamples(value int) int {
	if value <= 0 {
		return defaultDegradeMinSamples
	}
	return value
}

func normalizeDegradeThreshold(value, fallback float64) float64 {
	if value <= 0 || value > 1 {
		return fallback
	}
	return value
}

func normalizeDegradeNodeMinAccounts(value int) int {
	if value <= 0 {
		return defaultDegradeNodeMinAccounts
	}
	return value
}

func normalizeDegradeTokenLimit(value, fallback int64) int64 {
	if value <= 0 {
		return fallback
	}
	return value
}

func normalizeDegradeGenWindowMS(value int64) int64 {
	if value <= 0 {
		return defaultDegradeShapeMinGenerationMS
	}
	return value
}

func normalizeDegradeSuppressThreshold(value float64) float64 {
	if value <= 0 || value > 1 {
		return defaultDegradeGlobalSuppressThreshold
	}
	return value
}

func normalizeDegradeLadder(values []time.Duration) []time.Duration {
	result := make([]time.Duration, 0, len(values))
	for _, value := range values {
		if value > 0 {
			result = append(result, value)
		}
	}
	if len(result) == 0 {
		return defaultDegradeAccountLadder()
	}
	return result
}

func normalizeDegradeMaxStrikes(value int) int {
	if value <= 0 {
		return defaultDegradeMaxStrikes
	}
	return value
}

func normalizeDegradeCooldown(value, fallback time.Duration) time.Duration {
	if value <= 0 {
		return fallback
	}
	return value
}

// degradationSource 是退化事件来源：
//   - quality_withhold：质量守卫判定缺思考（标签命中，含 burst/cipher/missing-thinking）；
//   - upstream_stream_incomplete：上游流式截断（标签命中）；
//   - agent_empty_tool_turn：agent 空转窄规则（形状命中，需要按测量值分层）。
type degradationSource string

const (
	degradationSourceQualityWithhold    degradationSource = "quality_withhold"
	degradationSourceUpstreamTruncation degradationSource = "upstream_stream_incomplete"
	degradationSourceAgentEmptyToolTurn degradationSource = "agent_empty_tool_turn"
)

// degradationSourceIsLabelHit 判断来源是否属于「网关自身结论」的标签命中：
// 这类命中一律计入账号/节点统计，不受 input_tokens / gen_window 分层限制。
// hold 掉的流没有首字时间，gen_window=0 是「不可测」而不是测量伪影。
func degradationSourceIsLabelHit(source degradationSource) bool {
	switch source {
	case degradationSourceQualityWithhold, degradationSourceUpstreamTruncation:
		return true
	default:
		return false
	}
}

// degradationMode 控制灰度开关：
//   - observe（默认）：照常计算归因、命中统计与结构化日志，但不执行任何处置
//     副作用（不隔离租约、不冷却/禁用账号、不关闭节点调度）；
//   - enforce：按设计执行分类处置。
//
// fail_closed（退化响应绝不以 200 交付）与这个开关无关，始终生效。
const (
	degradationModeObserve = "observe"
	degradationModeEnforce = "enforce"
)

func normalizeDegradationMode(value string) string {
	if strings.EqualFold(strings.TrimSpace(value), degradationModeEnforce) {
		return degradationModeEnforce
	}
	return degradationModeObserve
}

// degradationReason 是归因/分类原因，与结构化日志的 reason 字段一致。
type degradationReason string

const (
	degradationReasonAccount degradationReason = "account"
	degradationReasonNode    degradationReason = "node"
	degradationReasonContext degradationReason = "context"
	degradationReasonShape   degradationReason = "shape"
	degradationReasonUnknown degradationReason = "unknown"
)

// 处置动作，仅用于结构化日志与测试断言。
const (
	degradationActionAccountCooldown  = "account_cooldown"
	degradationActionAccountDisabled  = "account_disabled"
	degradationActionLeaseQuarantined = "lease_quarantined"
	degradationActionNodeDisabled     = "node_disabled"
	// degradationActionObserveOnly 是 observe 模式日志里的 action 值，
	// 真实「如果 enforce 会做什么」写在 would_action 字段。
	degradationActionObserveOnly = "observe_only"
)

// degradationHit 是请求路径上归类后的一次退化命中。
type degradationHit struct {
	source degradationSource
	// withholdRule 是质量守卫的触发规则（仅 Withhold 来源有值），用于日志排查。
	withholdRule qualityWithholdRule
	accountID    uint64
	nodeID       uint64
	inputTokens  int64
	// genWindowMS = duration_ms - first_token_ms；-1 表示无法测量。
	genWindowMS int64
	at          time.Time
}

// degradationHitClass 是命中分层：只有 eligible 才进入归因统计与处置；
// context 只作为观测标签（不入统计、不影响处置），shape 是测量伪影。
type degradationHitClass int

const (
	degradationHitEligible degradationHitClass = iota
	degradationHitShape
	degradationHitContext
)

func newWithholdDegradationHit(credential accountdomain.Credential, trace *infraegress.Trace, provider accountdomain.Provider, usage Usage, evidence QualityStreamEvidence) degradationHit {
	genWindow := int64(-1)
	if evidence.HasVisible && evidence.VisibleFlushMS >= 0 {
		genWindow = evidence.VisibleFlushMS
	}
	// 只有 agent 空转窄规则是「形状命中」，需要按 gen_window/input 分层；
	// 其余 Withhold（burst/cipher/missing-thinking）都是标签命中，一律入统计。
	source := degradationSourceQualityWithhold
	if evidence.WithholdRule == qualityWithholdRuleEmptyToolTurn ||
		evidence.WithholdRule == qualityWithholdRuleEmptyAgentContext {
		source = degradationSourceAgentEmptyToolTurn
	}
	return degradationHit{
		source: source, withholdRule: evidence.WithholdRule, accountID: credential.ID,
		nodeID: degradationEgressNodeID(trace, provider), inputTokens: usage.InputTokens,
		genWindowMS: genWindow, at: time.Now().UTC(),
	}
}

func newTruncationDegradationHit(credential accountdomain.Credential, trace *infraegress.Trace, provider accountdomain.Provider, usage Usage, durationMS int64, firstTokenMS *int64) degradationHit {
	genWindow := int64(-1)
	if firstTokenMS != nil {
		genWindow = max(int64(0), durationMS-*firstTokenMS)
	}
	return degradationHit{
		source: degradationSourceUpstreamTruncation, accountID: credential.ID,
		nodeID: degradationEgressNodeID(trace, provider), inputTokens: usage.InputTokens,
		genWindowMS: genWindow, at: time.Now().UTC(),
	}
}

func (cfg QualityRetryRuntime) degradeMaxAttributionInputTokens() int64 {
	return normalizeDegradeTokenLimit(cfg.DegradeMaxAttributionInputTokens, defaultDegradeMaxAttributionInputTokens)
}

func (cfg QualityRetryRuntime) degradeShapeMinGenerationMS() int64 {
	return normalizeDegradeGenWindowMS(cfg.DegradeShapeMinGenerationMS)
}

// classifyDegradationHit 按来源分层决定是否进入归因统计：
//   - 标签命中（quality_withhold / upstream_stream_incomplete）一律 eligible：
//     标签是网关自身「缺思考/截断」的结论，不是 TPS 测量；hold 掉的流没有首字
//     时间，gen_window=0 是「不可测」而不是「测量伪影」。大上下文标签命中同样
//     计入统计，由全局抑制线兜底，而不是靠丢样本。
//   - 只有空转窄规则（agent_empty_tool_turn）是形状命中，才应用
//     input>=统计上限 → CONTEXT、gen_window<500ms → SHAPE 的跳过规则。
func (cfg QualityRetryRuntime) classifyDegradationHit(hit degradationHit) degradationHitClass {
	if degradationSourceIsLabelHit(hit.source) {
		return degradationHitEligible
	}
	if hit.inputTokens >= cfg.degradeMaxAttributionInputTokens() {
		return degradationHitContext
	}
	if hit.genWindowMS >= 0 && hit.genWindowMS < cfg.degradeShapeMinGenerationMS() {
		return degradationHitShape
	}
	return degradationHitEligible
}

func degradationReasonForHitClass(class degradationHitClass) degradationReason {
	switch class {
	case degradationHitContext:
		return degradationReasonContext
	case degradationHitShape:
		return degradationReasonShape
	default:
		return degradationReasonUnknown
	}
}

type degradationSample struct {
	at       time.Time
	degraded bool
}

type degradationPairKey struct {
	accountID uint64
	nodeID    uint64
}

// degradationCounter 保存一个键在滚动窗口内的样本。
type degradationCounter struct {
	samples []degradationSample
}

func (c *degradationCounter) trim(now time.Time, window time.Duration) {
	if c == nil {
		return
	}
	cutoff := now.Add(-window)
	expired := 0
	for expired < len(c.samples) && c.samples[expired].at.Before(cutoff) {
		expired++
	}
	if expired > 0 {
		c.samples = append(c.samples[:0], c.samples[expired:]...)
	}
	if overflow := len(c.samples) - degradeSamplesPerKey; overflow > 0 {
		c.samples = append(c.samples[:0], c.samples[overflow:]...)
	}
}

func (c *degradationCounter) record(now time.Time, degraded bool) {
	c.samples = append(c.samples, degradationSample{at: now, degraded: degraded})
}

func (c *degradationCounter) stats() (degraded, total int, rate float64) {
	if c == nil {
		return 0, 0, 0
	}
	for _, sample := range c.samples {
		total++
		if sample.degraded {
			degraded++
		}
	}
	if total > 0 {
		rate = float64(degraded) / float64(total)
	}
	return degraded, total, rate
}

func (c *degradationCounter) empty() bool { return c == nil || len(c.samples) == 0 }

// degradationEvidence 是一次归因计算的全部证据，同时用于结构化日志。
type degradationEvidence struct {
	decision   degradationReason
	window     time.Duration
	minSamples int
	// 账号自身证据。
	accountDegraded int
	accountTotal    int
	accountRate     float64
	accountBad      bool
	// 账号在多少个不同出口节点上达到账号阈值。
	accountBadNodes int
	// 当前出口节点证据。
	nodeDegraded         int
	nodeTotal            int
	nodeRate             float64
	nodeBad              bool
	nodeAccounts         int
	nodeAccountsDegraded int
	// 全局 baseline：全站普遍退化时抑制逐账号/节点处置。
	globalDegraded   int
	globalTotal      int
	globalRate       float64
	globalSuppressed bool
}

// degradationTracker 是进程内的滚动窗口统计。它只保存计数，不复制审计表；
// 多实例部署时每个实例独立归因（窗口状态本就属于请求路径的本地证据）。
type degradationTracker struct {
	mu       sync.Mutex
	accounts map[uint64]*degradationCounter
	nodes    map[uint64]*degradationCounter
	pairs    map[degradationPairKey]*degradationCounter
	// strikes 是账号冷却阶梯；accountNodes 记录账号被判 ACCOUNT 的不同出口节点，
	// 只有跨 ≥2 个节点确认后才允许升级为账号冷却/禁用。
	strikes      map[uint64]int
	accountNodes map[uint64]map[uint64]time.Time
	global       degradationCounter
}

func newDegradationTracker() *degradationTracker {
	return &degradationTracker{
		accounts:     make(map[uint64]*degradationCounter),
		nodes:        make(map[uint64]*degradationCounter),
		pairs:        make(map[degradationPairKey]*degradationCounter),
		strikes:      make(map[uint64]int),
		accountNodes: make(map[uint64]map[uint64]time.Time),
	}
}

func counterFor[K comparable](store map[K]*degradationCounter, key K) *degradationCounter {
	counter := store[key]
	if counter == nil {
		counter = &degradationCounter{}
		store[key] = counter
	}
	return counter
}

// record 记录一个样本（退化为 true，正常为 false）。窗口过期策略：每次记录与
// 归因都会裁掉窗口外的样本；空计数器在裁剪时删除，进程内存有界。
func (t *degradationTracker) record(event degradationHit, degraded bool, cfg QualityRetryRuntime) {
	if t == nil || event.accountID == 0 {
		return
	}
	window := cfg.degradeWindow()
	now := event.at
	if now.IsZero() {
		now = time.Now().UTC()
	}
	t.mu.Lock()
	defer t.mu.Unlock()
	t.pruneLocked(now, window)
	t.recordLocked(event, degraded, now, window)
}

func (t *degradationTracker) recordLocked(event degradationHit, degraded bool, now time.Time, window time.Duration) {
	account := counterFor(t.accounts, event.accountID)
	account.trim(now, window)
	account.record(now, degraded)
	t.global.trim(now, window)
	t.global.record(now, degraded)
	if event.nodeID == 0 {
		return
	}
	node := counterFor(t.nodes, event.nodeID)
	node.trim(now, window)
	node.record(now, degraded)
	pair := counterFor(t.pairs, degradationPairKey{accountID: event.accountID, nodeID: event.nodeID})
	pair.trim(now, window)
	pair.record(now, degraded)
}

// recordDegradedAndDecide 先记录退化样本再计算归因，避免并发事件互相穿插。
func (t *degradationTracker) recordDegradedAndDecide(event degradationHit, cfg QualityRetryRuntime) degradationEvidence {
	if t == nil {
		return degradationEvidence{decision: degradationReasonUnknown, window: cfg.degradeWindow(), minSamples: cfg.degradeMinSamples()}
	}
	window := cfg.degradeWindow()
	now := event.at
	if now.IsZero() {
		now = time.Now().UTC()
	}
	t.mu.Lock()
	defer t.mu.Unlock()
	t.pruneLocked(now, window)
	t.recordLocked(event, true, now, window)
	return t.decideLocked(event.accountID, event.nodeID, cfg)
}

func (t *degradationTracker) decideLocked(accountID, nodeID uint64, cfg QualityRetryRuntime) degradationEvidence {
	evidence := degradationEvidence{
		decision:   degradationReasonUnknown,
		window:     cfg.degradeWindow(),
		minSamples: cfg.degradeMinSamples(),
	}
	accountThreshold := cfg.degradeAccountThreshold()
	evidence.accountDegraded, evidence.accountTotal, evidence.accountRate = t.accounts[accountID].stats()
	evidence.accountBad = evidence.accountTotal >= evidence.minSamples && evidence.accountRate >= accountThreshold
	evidence.globalDegraded, evidence.globalTotal, evidence.globalRate = t.global.stats()
	if evidence.globalTotal >= evidence.minSamples*degradeGlobalSuppressMinSamplesFactor && evidence.globalRate >= cfg.degradeGlobalSuppressThreshold() {
		// 全站普遍退化：上游整体故障，不做逐账号/节点处置。
		evidence.globalSuppressed = true
		evidence.decision = degradationReasonUnknown
		return evidence
	}

	if evidence.accountBad {
		for key, counter := range t.pairs {
			if key.accountID != accountID {
				continue
			}
			_, total, rate := counter.stats()
			if total >= evidence.minSamples && rate >= accountThreshold {
				evidence.accountBadNodes++
			}
		}
	}
	if nodeID != 0 {
		evidence.nodeDegraded, evidence.nodeTotal, evidence.nodeRate = t.nodes[nodeID].stats()
		evidence.nodeBad = evidence.nodeTotal >= evidence.minSamples && evidence.nodeRate >= cfg.degradeNodeThreshold()
		if evidence.nodeBad {
			for key, counter := range t.pairs {
				if key.nodeID != nodeID {
					continue
				}
				_, total, rate := counter.stats()
				if total < evidence.minSamples {
					continue
				}
				evidence.nodeAccounts++
				if rate >= accountThreshold {
					evidence.nodeAccountsDegraded++
				}
			}
			if evidence.nodeAccounts < cfg.degradeNodeMinAccounts() {
				evidence.nodeBad = false
			}
			if evidence.nodeBad && evidence.nodeAccountsDegraded*2 < evidence.nodeAccounts {
				evidence.nodeBad = false
			}
		}
	}
	switch {
	case evidence.accountBad && evidence.accountBadNodes >= 2:
		// 同一账号在 ≥2 个不同出口节点上退化：跨节点确认，问题跟随账号。
		evidence.decision = degradationReasonAccount
	case evidence.nodeBad:
		// 节点整体退化且多数账号同样退化：问题跟随出口节点。
		evidence.decision = degradationReasonNode
	case evidence.accountBad:
		// 单节点账号证据：先隔离该账号在当前节点的租约，跨节点后才升级。
		evidence.decision = degradationReasonAccount
	default:
		evidence.decision = degradationReasonUnknown
	}
	return evidence
}

func (t *degradationTracker) pruneLocked(now time.Time, window time.Duration) {
	cutoff := now.Add(-window)
	for key, counter := range t.accounts {
		counter.trim(now, window)
		if counter.empty() {
			delete(t.accounts, key)
		}
	}
	for key, counter := range t.nodes {
		counter.trim(now, window)
		if counter.empty() {
			delete(t.nodes, key)
		}
	}
	for key, counter := range t.pairs {
		counter.trim(now, window)
		if counter.empty() {
			delete(t.pairs, key)
		}
	}
	for accountID, nodes := range t.accountNodes {
		for nodeID, at := range nodes {
			if at.Before(cutoff) {
				delete(nodes, nodeID)
			}
		}
		if len(nodes) == 0 {
			delete(t.accountNodes, accountID)
		}
	}
	t.global.trim(now, window)
}

// noteAccountNodeJudgement 记录一次「账号被判定为 ACCOUNT」的出口节点，返回窗口内
// 该账号被判定过的不同节点数。只有 ≥2 个节点才允许升级为账号冷却/禁用。
func (t *degradationTracker) noteAccountNodeJudgement(accountID, nodeID uint64, now time.Time, window time.Duration) int {
	if t == nil || accountID == 0 || nodeID == 0 {
		return 0
	}
	t.mu.Lock()
	defer t.mu.Unlock()
	nodes := t.accountNodes[accountID]
	if nodes == nil {
		nodes = make(map[uint64]time.Time)
		t.accountNodes[accountID] = nodes
	}
	nodes[nodeID] = now
	cutoff := now.Add(-window)
	count := 0
	for id, at := range nodes {
		if at.Before(cutoff) {
			delete(nodes, id)
			continue
		}
		count++
	}
	if len(nodes) == 0 {
		delete(t.accountNodes, accountID)
	}
	return count
}

// accountNodeJudgementPlan 计算「把当前节点算进去后」该账号在窗口内被判定为
// ACCOUNT 的不同节点数，但不修改状态。observe 模式用它推断 would_action。
func (t *degradationTracker) accountNodeJudgementPlan(accountID, nodeID uint64, now time.Time, window time.Duration) int {
	if t == nil || accountID == 0 || nodeID == 0 {
		return 0
	}
	t.mu.Lock()
	defer t.mu.Unlock()
	cutoff := now.Add(-window)
	seen := make(map[uint64]struct{}, 2)
	for id, at := range t.accountNodes[accountID] {
		if at.Before(cutoff) {
			continue
		}
		seen[id] = struct{}{}
	}
	seen[nodeID] = struct{}{}
	return len(seen)
}

// accountStrikes 返回账号当前连续归因命中次数。
func (t *degradationTracker) accountStrikes(accountID uint64) int {
	if t == nil {
		return 0
	}
	t.mu.Lock()
	defer t.mu.Unlock()
	return t.strikes[accountID]
}

// bumpAccountStrikes 累加一次账号归因命中。
func (t *degradationTracker) bumpAccountStrikes(accountID uint64) int {
	if t == nil {
		return 0
	}
	t.mu.Lock()
	defer t.mu.Unlock()
	t.strikes[accountID]++
	return t.strikes[accountID]
}

// clearAccountStrikes 清空账号冷却阶梯与跨节点判定记录。账号一次正常（带推理
// 证据）交付后调用，或 NODE 归因确认账号无责时调用。
func (t *degradationTracker) clearAccountStrikes(accountID uint64) {
	if t == nil {
		return
	}
	t.mu.Lock()
	defer t.mu.Unlock()
	delete(t.strikes, accountID)
	delete(t.accountNodes, accountID)
}

// degradeWindow 等方法把归一化默认值封装在运行时配置上，测试可直接用零值配置。
func (cfg QualityRetryRuntime) degradeWindow() time.Duration {
	return normalizeDegradeWindow(cfg.DegradeWindow)
}

func (cfg QualityRetryRuntime) degradeMinSamples() int {
	return normalizeDegradeMinSamples(cfg.DegradeMinSamples)
}

func (cfg QualityRetryRuntime) degradeAccountThreshold() float64 {
	return normalizeDegradeThreshold(cfg.DegradeAccountThreshold, defaultDegradeAccountThreshold)
}

func (cfg QualityRetryRuntime) degradeNodeThreshold() float64 {
	return normalizeDegradeThreshold(cfg.DegradeNodeThreshold, defaultDegradeNodeThreshold)
}

func (cfg QualityRetryRuntime) degradeNodeMinAccounts() int {
	return normalizeDegradeNodeMinAccounts(cfg.DegradeNodeMinAccounts)
}

func (cfg QualityRetryRuntime) degradeMaxStrikes() int {
	return normalizeDegradeMaxStrikes(cfg.DegradeMaxStrikes)
}

func (cfg QualityRetryRuntime) degradeAccountLadder() []time.Duration {
	return normalizeDegradeLadder(cfg.DegradeAccountCooldowns)
}

func (cfg QualityRetryRuntime) degradeQuarantineDuration() time.Duration {
	return normalizeDegradeCooldown(cfg.DegradeQuarantineDuration, defaultDegradeQuarantineDuration)
}

func (cfg QualityRetryRuntime) degradeUnknownCooldown() time.Duration {
	return normalizeDegradeCooldown(cfg.DegradeUnknownCooldown, defaultDegradeUnknownCooldown)
}

func (cfg QualityRetryRuntime) degradeGlobalSuppressThreshold() float64 {
	return normalizeDegradeSuppressThreshold(cfg.DegradeGlobalSuppressThreshold)
}

// degradationMode 返回归一化后的灰度开关：observe（默认）或 enforce。
func (cfg QualityRetryRuntime) degradationMode() string {
	return normalizeDegradationMode(cfg.DegradationMode)
}

// degradeObserveOnly 表示当前只观测归因、不执行处置副作用。
func (cfg QualityRetryRuntime) degradeObserveOnly() bool {
	return cfg.degradationMode() != degradationModeEnforce
}

// degradeLadderCooldown 返回第 strikes 次连续命中对应的冷却时长（从 1 开始计数），
// 超出阶梯长度后重复最后一档。
func (cfg QualityRetryRuntime) degradeLadderCooldown(strikes int) time.Duration {
	ladder := cfg.degradeAccountLadder()
	if strikes < 1 {
		strikes = 1
	}
	if strikes > len(ladder) {
		strikes = len(ladder)
	}
	return ladder[strikes-1]
}

// egressLeaseQuarantiner 复用 account_egress_lease_blocks 既有仓储方法
// （即管理端 /egress-leases/quarantine 背后的 UpsertEgressLeaseBlock），
// 只隔离一个「账号×节点」租约，不改账号健康、不动节点健康，也不新建存储。
type egressLeaseQuarantiner interface {
	UpsertEgressLeaseBlock(context.Context, accountdomain.EgressLeaseBlock) (accountdomain.EgressLeaseBlock, error)
}

// egressNodeDisabler 复用 egress 运营服务的批量调度开关（UpdateManyEnabled）。
// 它只改 Enabled，不动代理密钥、健康度、探测状态与账号绑定。
type egressNodeDisabler interface {
	UpdateManyEnabled(context.Context, []uint64, bool) (int, error)
}

// egressNodeLookup 只读节点元数据，用于结构化日志里补 egress_node_name，
// 便于灰度期人工复核 NODE 处置（不参与处置逻辑）。
type egressNodeLookup interface {
	GetEgressNode(context.Context, uint64) (egressdomain.Node, error)
}

// SetEgressLeaseQuarantiner 注入账号×节点租约隔离仓储。装配在应用启动时完成。
func (s *Service) SetEgressLeaseQuarantiner(quarantiner egressLeaseQuarantiner) {
	s.egressLeases = quarantiner
}

// SetEgressNodeDisabler 注入节点调度开关。装配在应用启动时完成。
func (s *Service) SetEgressNodeDisabler(disabler egressNodeDisabler) {
	s.egressNodes = disabler
}

// SetEgressNodeLookup 注入只读节点元数据查询（仅用于日志里的节点名称）。
func (s *Service) SetEgressNodeLookup(lookup egressNodeLookup) {
	s.egressNodeLookup = lookup
}

// degradationNodeName 返回节点名称用于日志；查询失败返回空串，不影响处置。
func (s *Service) degradationNodeName(ctx context.Context, nodeID uint64) string {
	if nodeID == 0 || s.egressNodeLookup == nil {
		return ""
	}
	node, err := s.egressNodeLookup.GetEgressNode(ctx, nodeID)
	if err != nil {
		return ""
	}
	return node.Name
}

// degradationLeaseAvailable 判断当前账号/节点是否支持账号×节点租约隔离：
// 仓储只支持 Build 账号，direct 出口（nodeID==0）与非 Build 账号没有租约。
func (s *Service) degradationLeaseAvailable(credential accountdomain.Credential, nodeID uint64) bool {
	return nodeID != 0 && s.egressLeases != nil && credential.Provider == accountdomain.ProviderBuild
}

// degradationEgressNodeID 从本次请求实际使用的出口选择里取节点 ID；direct（无
// 代理）与未记录选择都返回 0，此时不做节点归因。
func degradationEgressNodeID(trace *infraegress.Trace, provider accountdomain.Provider) uint64 {
	selection, ok := trace.Selection(primaryEgressScope(provider))
	if !ok {
		return 0
	}
	return selection.NodeID
}

// qualityRequestDeclaresTools 只读检查请求体是否声明了非空 tools。每个请求只
// 解析一次请求体，不参与逐帧扫描。
func qualityRequestDeclaresTools(body []byte) bool {
	if len(body) == 0 {
		return false
	}
	var payload struct {
		Tools json.RawMessage `json:"tools"`
	}
	if json.Unmarshal(body, &payload) != nil || len(payload.Tools) == 0 {
		return false
	}
	var tools []json.RawMessage
	if json.Unmarshal(payload.Tools, &tools) != nil {
		return false
	}
	return len(tools) > 0
}

// observeDegradationHit 处理一次已分层的退化命中：
//   - eligible：记录滚动样本、计算归因并（按 degradationMode）执行或仅观测处置；
//   - context/shape：只写结构化观测日志（context_large 标签），不进入统计、
//     不惩罚账号或节点；请求的重试/失败路径完全不受影响。
func (s *Service) observeDegradationHit(ctx context.Context, requestID string, credential accountdomain.Credential, hit degradationHit, usage Usage, cfg QualityRetryRuntime) degradationReason {
	class := cfg.classifyDegradationHit(hit)
	if class == degradationHitContext || class == degradationHitShape {
		// 这两类命中不惩罚任何对象：CONTEXT 是「输入很大」的观测标签
		// （线上 130k 输入 92% 无推理是相关而非因果，待后续定位），
		// SHAPE 是缓冲倾倒/测量伪影。只有空转窄规则（形状命中）会走到这里；
		// withhold/截断等标签命中一律进入统计，不会被告警为 skipped。
		s.logger.Info("degradation_skipped",
			"request_id", requestID, "source", string(hit.source),
			"withhold_rule", string(hit.withholdRule),
			"reason", string(degradationReasonForHitClass(class)),
			"skip_dimension", string(degradationReasonForHitClass(class)),
			"label_hit", degradationSourceIsLabelHit(hit.source),
			"mode", cfg.degradationMode(),
			"context_large", hit.inputTokens >= cfg.degradeMaxAttributionInputTokens(),
			"account_id", credential.ID, "egress_node_id", hit.nodeID,
			"egress_node_name", s.degradationNodeName(ctx, hit.nodeID),
			"input_tokens", hit.inputTokens, "gen_window_ms", hit.genWindowMS,
			"output_tokens", usage.OutputTokens, "reasoning_tokens", usage.ReasoningTokens,
			"max_attribution_input_tokens", cfg.degradeMaxAttributionInputTokens(),
		)
		return degradationReasonForHitClass(class)
	}
	evidence, action, wouldAction, err := s.applyDegradationDisposition(ctx, credential, hit, cfg)
	if err != nil {
		s.logger.Error("degradation_penalty_failed",
			"request_id", requestID, "source", string(hit.source), "mode", cfg.degradationMode(),
			"account_id", credential.ID, "egress_node_id", hit.nodeID,
			"reason", string(evidence.decision), "action", action, "would_action", wouldAction, "error", err)
	}
	// 归因证据、处置动作、节点 id/名称与触发依据（退化率/样本数/账号数）
	// 统一写在一条日志里，灰度期可直接按 reason/action/would_action 聚合复核。
	s.logger.Info("degradation_attributed",
		"request_id", requestID,
		"source", string(hit.source),
		"withhold_rule", string(hit.withholdRule),
		"label_hit", degradationSourceIsLabelHit(hit.source),
		"mode", cfg.degradationMode(),
		"reason", string(evidence.decision),
		"action", action,
		"would_action", wouldAction,
		"context_large", hit.inputTokens >= cfg.degradeMaxAttributionInputTokens(),
		"account_id", credential.ID,
		"egress_node_id", hit.nodeID,
		"egress_node_name", s.degradationNodeName(ctx, hit.nodeID),
		"input_tokens", hit.inputTokens,
		"gen_window_ms", hit.genWindowMS,
		"output_tokens", usage.OutputTokens,
		"reasoning_tokens", usage.ReasoningTokens,
		"account_samples", evidence.accountTotal,
		"account_degraded", evidence.accountDegraded,
		"account_degrade_rate", evidence.accountRate,
		"account_bad_nodes", evidence.accountBadNodes,
		"node_samples", evidence.nodeTotal,
		"node_degraded", evidence.nodeDegraded,
		"node_degrade_rate", evidence.nodeRate,
		"node_accounts", evidence.nodeAccounts,
		"node_accounts_degraded", evidence.nodeAccountsDegraded,
		"global_degrade_rate", evidence.globalRate,
		"global_suppressed", evidence.globalSuppressed,
		"window", evidence.window.String(),
		"min_samples", evidence.minSamples,
	)
	return evidence.decision
}

// applyDegradationDisposition 记录退化样本、计算归因，并按当前 degradationMode
// 执行处置（enforce）或只计算 would_action（observe，无任何副作用）。
func (s *Service) applyDegradationDisposition(ctx context.Context, credential accountdomain.Credential, hit degradationHit, cfg QualityRetryRuntime) (degradationEvidence, string, string, error) {
	evidence := s.degrade.recordDegradedAndDecide(hit, cfg)
	writeCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), finalizationTimeout)
	defer cancel()
	action, wouldAction, err := s.degradationPenalty(writeCtx, credential, hit, evidence, cfg)
	return evidence, action, wouldAction, err
}

// degradationPlan 是 enforce 模式下打算执行的单个处置动作；observe 模式只把它
// 写进 would_action，不产生副作用。
type degradationPlan struct {
	action   string
	escalate bool
}

// degradationPenalty 按归因结论计算/执行分类处置：
//   - ACCOUNT：第一档只隔离该账号在当前出口节点的租约；同一账号在 ≥2 个不同
//     节点被判定后才升级为账号冷却阶梯（连续命中 maxStrikes 后禁用，原因
//     degraded_account）；
//   - NODE：只关闭节点调度（UpdateManyEnabled(false)），不惩罚账号、不写节点
//     健康度；关闭失败（例如固定回退节点）时退化为隔离当前账号×节点租约；
//   - UNKNOWN：只施加一次短冷却。
//
// 返回 (实际动作, enforce 会执行的动作)。observe 模式下实际动作为 observe_only
// 且没有任何副作用（不写租约、不写账号健康、不关节点、不消费 strike）。
func (s *Service) degradationPenalty(ctx context.Context, credential accountdomain.Credential, hit degradationHit, evidence degradationEvidence, cfg QualityRetryRuntime) (string, string, error) {
	observeOnly := cfg.degradeObserveOnly()
	switch evidence.decision {
	case degradationReasonAccount:
		plan := s.planDegradedAccount(credential, hit, evidence, cfg)
		if observeOnly {
			return degradationActionObserveOnly, plan.action, nil
		}
		action, err := s.executeDegradedAccount(ctx, credential, hit, cfg, plan)
		return action, plan.action, err
	case degradationReasonNode:
		plan := s.planDegradedNode(hit)
		if observeOnly {
			return degradationActionObserveOnly, plan.action, nil
		}
		action, err := s.executeDegradedNode(ctx, credential, hit, cfg)
		return action, plan.action, err
	default:
		if observeOnly {
			return degradationActionObserveOnly, degradationActionAccountCooldown, nil
		}
		action, err := s.selector.markDegradedAccount(ctx, credential, cfg.degradeUnknownCooldown(), false)
		return action, degradationActionAccountCooldown, err
	}
}

// planDegradedAccount 计算 ACCOUNT 处置会做什么（不修改任何状态）。
func (s *Service) planDegradedAccount(credential accountdomain.Credential, hit degradationHit, evidence degradationEvidence, cfg QualityRetryRuntime) degradationPlan {
	now := time.Now().UTC()
	judgedNodes := s.degrade.accountNodeJudgementPlan(credential.ID, hit.nodeID, now, cfg.degradeWindow())
	escalate := evidence.accountBadNodes >= 2 || judgedNodes >= 2
	if !escalate {
		if s.degradationLeaseAvailable(credential, hit.nodeID) {
			return degradationPlan{action: degradationActionLeaseQuarantined}
		}
		// 无可用租约（direct 出口、非 Build 账号或节点缺失）：第一档退化为短冷却。
		return degradationPlan{action: degradationActionAccountCooldown}
	}
	strikes := s.degrade.accountStrikes(credential.ID) + 1
	action := degradationActionAccountCooldown
	if strikes >= cfg.degradeMaxStrikes() {
		action = degradationActionAccountDisabled
	}
	return degradationPlan{action: action, escalate: true}
}

// executeDegradedAccount 执行 ACCOUNT 处置（仅 enforce 模式调用）。
func (s *Service) executeDegradedAccount(ctx context.Context, credential accountdomain.Credential, hit degradationHit, cfg QualityRetryRuntime, plan degradationPlan) (string, error) {
	if !plan.escalate {
		action, err := s.quarantineDegradedLease(ctx, credential, hit.nodeID, cfg.degradeQuarantineDuration(), "degraded_account")
		if err != nil || action != degradationActionNone {
			return action, err
		}
		// 租约不可用（仓储冲突/非 Build）：退化为短冷却，不直接禁用账号。
		return s.selector.markDegradedAccount(ctx, credential, cfg.degradeUnknownCooldown(), false)
	}
	// enforce 路径以真实累计 strike 为准（与 plan 的 +1 预演一致）。
	s.degrade.noteAccountNodeJudgement(credential.ID, hit.nodeID, time.Now().UTC(), cfg.degradeWindow())
	strikes := s.degrade.bumpAccountStrikes(credential.ID)
	disable := strikes >= cfg.degradeMaxStrikes()
	return s.selector.markDegradedAccount(ctx, credential, cfg.degradeLadderCooldown(strikes), disable)
}

// planDegradedNode 计算 NODE 处置会做什么（不修改任何状态）。
func (s *Service) planDegradedNode(hit degradationHit) degradationPlan {
	if hit.nodeID == 0 || s.egressNodes == nil {
		return degradationPlan{action: degradationActionNone}
	}
	return degradationPlan{action: degradationActionNodeDisabled}
}

// executeDegradedNode 执行 NODE 处置（仅 enforce 模式调用）：
// 先清掉该账号 strike（账号无责），再关闭节点调度；关闭失败时退化为租约隔离。
func (s *Service) executeDegradedNode(ctx context.Context, credential accountdomain.Credential, hit degradationHit, cfg QualityRetryRuntime) (string, error) {
	s.degrade.clearAccountStrikes(credential.ID)
	action, err := s.disableDegradedEgressNode(ctx, hit.nodeID)
	if err == nil && action == degradationActionNone {
		return s.quarantineDegradedLease(ctx, credential, hit.nodeID, cfg.degradeQuarantineDuration(), "degraded_node")
	}
	return action, err
}

// quarantineDegradedLease 只隔离一个账号×节点租约（account_egress_lease_blocks），
// 不写账号健康、不写节点健康。返回 none 表示当前环境不支持租约隔离。
func (s *Service) quarantineDegradedLease(ctx context.Context, credential accountdomain.Credential, nodeID uint64, cooldown time.Duration, reason string) (string, error) {
	if nodeID == 0 || s.egressLeases == nil || credential.Provider != accountdomain.ProviderBuild {
		return degradationActionNone, nil
	}
	version, err := security.NewOpaqueToken(18)
	if err != nil {
		return degradationActionNone, err
	}
	now := time.Now().UTC()
	_, err = s.egressLeases.UpsertEgressLeaseBlock(ctx, accountdomain.EgressLeaseBlock{
		AccountID: credential.ID, NodeID: nodeID, Reason: reason, Version: version,
		CooldownUntil: now.Add(cooldown), UpdatedAt: now,
	})
	if errors.Is(err, repository.ErrConflict) {
		// 账号已失效、非 Build 或节点绑定不匹配：租约隔离不适用，交给上游处置。
		return degradationActionNone, nil
	}
	if err != nil {
		return degradationActionNone, err
	}
	s.logger.Info("degradation_lease_quarantined", "account_id", credential.ID, "egress_node_id", nodeID, "egress_node_name", s.degradationNodeName(ctx, nodeID), "reason", reason, "cooldown", cooldown.String())
	return degradationActionLeaseQuarantined, nil
}

// disableDegradedEgressNode 关闭节点调度（Enabled=false）。注意这里刻意不写
// 节点健康度/探测状态：AutoRemoveUnhealthyNodes 会删除 health<0.2 的节点，
// 单节点绑定的 215+ 账号会被连带解绑。关闭后需要运维确认再手动启用。
func (s *Service) disableDegradedEgressNode(ctx context.Context, nodeID uint64) (string, error) {
	if nodeID == 0 || s.egressNodes == nil {
		return degradationActionNone, nil
	}
	updated, err := s.egressNodes.UpdateManyEnabled(ctx, []uint64{nodeID}, false)
	if err != nil {
		s.logger.Warn("degradation_egress_node_disable_failed", "egress_node_id", nodeID, "egress_node_name", s.degradationNodeName(ctx, nodeID), "error", err)
		return degradationActionNone, err
	}
	if updated == 0 {
		return degradationActionNone, nil
	}
	s.logger.Warn("degradation_egress_node_disabled", "egress_node_id", nodeID, "egress_node_name", s.degradationNodeName(ctx, nodeID))
	return degradationActionNodeDisabled, nil
}

// markDegradedAccount 复用账号健康存储：冷却写 UpdateHealth + 运行时失效广播，
// 禁用复用 disableAccount，两者都写入 degraded_account 健康标记。
func (s *Selector) markDegradedAccount(ctx context.Context, credential accountdomain.Credential, cooldown time.Duration, disable bool) (string, error) {
	now := time.Now().UTC()
	if disable {
		if err := s.disableAccount(ctx, credential); err != nil {
			return degradationActionNone, err
		}
		if err := s.accounts.UpdateHealth(ctx, credential.ID, credential.Provider, credential.FailureCount, nil, accountdomain.LastErrorDegradedAccount, false); err != nil {
			return degradationActionAccountDisabled, err
		}
		return degradationActionAccountDisabled, nil
	}
	until := now.Add(cooldown)
	// 不缩短账号已有的更长冷却（例如 legacy 冷却或上一条阶梯）；只延长。
	if credential.CooldownUntil != nil && credential.CooldownUntil.After(until) {
		until = *credential.CooldownUntil
	}
	if err := s.accounts.UpdateHealth(ctx, credential.ID, credential.Provider, credential.FailureCount, &until, accountdomain.LastErrorDegradedAccount, false); err != nil {
		return degradationActionNone, err
	}
	s.ApplyInvalidation(repository.InvalidationEvent{
		Kind: repository.InvalidationAccountHealthChanged, Provider: credential.Provider, AccountID: credential.ID,
		FailureCount: credential.FailureCount, CooldownUntil: &until, HealthMarker: accountdomain.LastErrorDegradedAccount,
	})
	s.evictCandidate(credential.Provider, credential.ID)
	return degradationActionAccountCooldown, nil
}

// observeHealthyQualitySample 记录一次正常交付样本，并在观察到真实推理证据时
// 清空账号冷却阶梯（「账号一次正常有推理成功则清零阶梯」）。正常样本参与比例
// 分母，因此窗口内的退化率需要正负样本共同支撑。
//
// 注意：正常样本不做 input_tokens 过滤——标签命中（含大上下文）一律进入统计，
// 分母也必须包含同样口径的正常样本，否则大上下文流量会系统性地推高退化率。
func (s *Service) observeHealthyQualitySample(credential accountdomain.Credential, trace *infraegress.Trace, provider accountdomain.Provider, usage Usage, evidence QualityStreamEvidence) {
	_ = usage
	cfg := s.qualityRetryConfig()
	if !cfg.DegradeAttributionEnabled {
		return
	}
	hit := degradationHit{accountID: credential.ID, nodeID: degradationEgressNodeID(trace, provider), at: time.Now().UTC()}
	s.degrade.record(hit, false, cfg)
	if evidence.HasThinking {
		s.degrade.clearAccountStrikes(credential.ID)
	}
}
