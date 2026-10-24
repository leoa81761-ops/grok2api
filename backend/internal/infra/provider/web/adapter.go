package web

import (
	"context"
	"log/slog"
	"strings"
	"sync"

	"github.com/chenyme/grok2api/backend/internal/domain/account"
	modeldomain "github.com/chenyme/grok2api/backend/internal/domain/model"
	settingsdomain "github.com/chenyme/grok2api/backend/internal/domain/settings"
	infraegress "github.com/chenyme/grok2api/backend/internal/infra/egress"
	"github.com/chenyme/grok2api/backend/internal/infra/provider"
	"github.com/chenyme/grok2api/backend/internal/infra/security"
	"github.com/chenyme/grok2api/backend/internal/repository"
)

type Config struct {
	BaseURL string
	// PublicBaseURL 是本服务对外的公开地址（配置项 publicBaseURL）。
	// 用于识别「帧图/参考图 URL 指向网关自己」的请求：这类下载优先读本地媒体
	// 存储（零网络）。所有非本地图片下载都走直连，不经过账号出口代理。
	PublicBaseURL            string
	StatsigMode              string
	StatsigManualValue       string
	StatsigSignerURL         string
	QuotaTimeoutSeconds      int
	ChatTimeoutSeconds       int
	StreamIdleTimeoutSeconds int
	ImageTimeoutSeconds      int
	VideoTimeoutSeconds      int
	MaxInputImageBytes       int64
	AllowNSFW                bool
	FreeVideoDurationCap     int
}

type Adapter struct {
	mu              sync.RWMutex
	cfg             Config
	accountsBaseURL string
	egress          *infraegress.Manager
	cipher          *security.Cipher
	states          repository.ResponseRepository
	assets          provider.ImageAssetStore
	statsig         *statsigSigner
	// directImages 下载所有非本地图片（直连、不经出口代理）。
	// 单测注入它来断言图片下载不会进入 egress 链路。
	directImages imageDirectLoader
	logger       *slog.Logger
}

func NewAdapter(cfg Config, egress *infraegress.Manager, cipher *security.Cipher, states repository.ResponseRepository, assets provider.ImageAssetStore) *Adapter {
	cfg = normalizedConfig(cfg)
	return &Adapter{cfg: cfg, accountsBaseURL: officialAccountsBaseURL, egress: egress, cipher: cipher, states: states, assets: assets, statsig: newStatsigSigner(), directImages: directImageLoader{}, logger: slog.Default()}
}

func (a *Adapter) SetLogger(logger *slog.Logger) {
	if logger != nil {
		a.logger = logger
	}
}

func (a *Adapter) log() *slog.Logger {
	if a.logger != nil {
		return a.logger
	}
	return slog.Default()
}

func normalizedConfig(cfg Config) Config {
	if cfg.BaseURL == "" {
		cfg.BaseURL = "https://grok.com"
	}
	cfg.PublicBaseURL = strings.TrimRight(strings.TrimSpace(cfg.PublicBaseURL), "/")
	if cfg.StatsigMode == "" {
		cfg.StatsigMode = "url"
	}
	if cfg.StatsigSignerURL == "" {
		cfg.StatsigSignerURL = defaultStatsigSignerURL
	}
	if cfg.QuotaTimeoutSeconds <= 0 {
		cfg.QuotaTimeoutSeconds = 25
	}
	if cfg.ChatTimeoutSeconds <= 0 {
		cfg.ChatTimeoutSeconds = 120
	}
	if cfg.StreamIdleTimeoutSeconds <= 0 {
		cfg.StreamIdleTimeoutSeconds = int(settingsdomain.DefaultWebStreamIdleTimeout.Seconds())
	}
	if cfg.ImageTimeoutSeconds <= 0 {
		cfg.ImageTimeoutSeconds = 180
	}
	if cfg.VideoTimeoutSeconds <= 0 {
		cfg.VideoTimeoutSeconds = 900
	}
	if cfg.MaxInputImageBytes <= 0 {
		cfg.MaxInputImageBytes = 32 << 20
	}
	cfg.FreeVideoDurationCap = normalizeFreeVideoDurationCap(cfg.FreeVideoDurationCap)
	return cfg
}

func (a *Adapter) UpdateConfig(cfg Config) {
	cfg = normalizedConfig(cfg)
	a.mu.Lock()
	changed := a.cfg.StatsigMode != cfg.StatsigMode || a.cfg.StatsigManualValue != cfg.StatsigManualValue || a.cfg.StatsigSignerURL != cfg.StatsigSignerURL || a.cfg.BaseURL != cfg.BaseURL
	a.cfg = cfg
	a.mu.Unlock()
	if changed && a.statsig != nil {
		a.statsig.Clear()
	}
}

func (a *Adapter) config() Config {
	a.mu.RLock()
	defer a.mu.RUnlock()
	return a.cfg
}

func (a *Adapter) Provider() account.Provider { return account.ProviderWeb }

func (a *Adapter) QuotaMode(upstreamModel string) string {
	if spec, ok := Resolve(upstreamModel); ok {
		return spec.Mode
	}
	return ""
}

func (a *Adapter) QuotaRefreshGroup(upstreamModel string) string {
	if spec, ok := Resolve(upstreamModel); ok && account.IsWebImagineQuotaMode(spec.Mode) {
		return account.QuotaGroupWebImagine
	}
	return ""
}

func (a *Adapter) TierOrder(upstreamModel string) []account.WebTier {
	spec, ok := Resolve(upstreamModel)
	if !ok {
		return nil
	}
	switch spec.MinimumTier {
	case account.WebTierHeavy:
		return []account.WebTier{account.WebTierHeavy}
	case account.WebTierSuper:
		return []account.WebTier{account.WebTierSuper, account.WebTierHeavy}
	default:
		return []account.WebTier{account.WebTierBasic, account.WebTierSuper, account.WebTierHeavy}
	}
}

func (a *Adapter) TierOrderForQuotaMode(upstreamModel, quotaMode string) []account.WebTier {
	order := a.TierOrder(upstreamModel)
	spec, ok := Resolve(upstreamModel)
	if !ok || spec.Capability != modeldomain.CapabilityVideo || quotaMode == account.QuotaModeWebVideo720p {
		return order
	}
	// Basic video entitlement is currently confirmed only for the default
	// 720p product. Other video quota products remain on paid Web tiers until
	// independently verified upstream.
	filtered := make([]account.WebTier, 0, len(order))
	for _, tier := range order {
		if tier != account.WebTierBasic {
			filtered = append(filtered, tier)
		}
	}
	return filtered
}

func (a *Adapter) PricingModel(upstreamModel string) string {
	spec, ok := Resolve(upstreamModel)
	if ok {
		if spec.Capability == modeldomain.CapabilityChat {
			return "grok-4.5"
		}
		// Lite keeps its historical upstream billing name; Imagine WebSocket
		// models use their canonical public product name rather than the internal
		// compatibility identifier used to preserve route IDs.
		if spec.Capability == modeldomain.CapabilityImage {
			if spec.ProtocolModel == "imagine" {
				return spec.PublicID
			}
			return spec.UpstreamModel
		}
		// Dedicated Web edit upstream keeps a stable billing name.
		if spec.Capability == modeldomain.CapabilityImageEdit {
			return "grok-imagine-image-edit"
		}
		return spec.PublicID
	}
	return upstreamModel
}

func (a *Adapter) ListModels(_ context.Context, credential account.Credential) ([]string, error) {
	tier := credential.WebTier
	if tier == "" || tier == account.WebTierAuto || tier == account.WebTierBasic {
		tier = account.WebTierBasic
	}
	values := make([]string, 0, len(catalog))
	for _, spec := range catalog {
		if TierSupports(tier, spec.MinimumTier) {
			values = append(values, spec.UpstreamModel)
		}
	}
	return values, nil
}
