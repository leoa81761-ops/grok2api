package web

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strings"
	"time"

	mediadomain "github.com/chenyme/grok2api/backend/internal/domain/media"
	infraegress "github.com/chenyme/grok2api/backend/internal/infra/egress"
	"github.com/chenyme/grok2api/backend/internal/infra/provider"
)

// localImageAssetReader 读取本服务自己保存的公开图库图片。
// 生产实现是 application/media.Service；这里只依赖读接口，避免 infra 反向依赖 application。
type localImageAssetReader interface {
	OpenImage(ctx context.Context, id string) (mediadomain.Asset, io.ReadCloser, error)
}

// imageDirectLoader 下载「非本地资产」的图片（自有媒体 URL 的兜底、外部图床的
// 首帧/尾帧、图片编辑参考图）。
//
// 一律直连、绝不经过账号出口代理：下载图片是网关自己的出站抓取，与 grok 账号
// IP 无关；而 web 出口代理是为访问 grok.com 设计的，访问任意第三方站点既不可靠
// （实测同一 URL 从主机直连 0.17s 200，经出口代理却 context deadline exceeded）
// 也没有必要占用账号出口。
type imageDirectLoader interface {
	LoadImage(ctx context.Context, rawURL string, maxBytes int64) (provider.ImageInput, error)
}

// directImageLoader 是 imageDirectLoader 的生产实现：复用对话图片下载的 SSRF
// 校验（DNS 结果必须全为公网地址、URL 固定到已校验 IP、禁止重定向），传输层不
// 配置任何出口代理，TLS 仍用原始主机名做 SNI。重定向一律拒绝，避免跟随到未经
// 校验的地址（picsum 这类 302 图床会直接报错提示改用直链）。
type directImageLoader struct{}

func (directImageLoader) LoadImage(ctx context.Context, rawURL string, maxBytes int64) (provider.ImageInput, error) {
	target, err := validateRemoteImageURL(ctx, rawURL)
	if err != nil {
		return provider.ImageInput{}, err
	}
	requestCtx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()
	request, err := http.NewRequestWithContext(requestCtx, http.MethodGet, target.fetchURL.String(), nil)
	if err != nil {
		return provider.ImageInput{}, err
	}
	request.Host = target.hostHeader
	// 这是网关自己的出站抓取，不携带任何账号 Cookie/SSO。
	request.Header = remoteImageHeaders(infraegress.DefaultUserAgent)
	response, err := infraegress.DoPinnedHTTPSDirect(request, target.serverName)
	if err != nil {
		return provider.ImageInput{}, fmt.Errorf("直连下载图片: %w", err)
	}
	defer response.Body.Close()
	return readDirectImageResponse(response, target.originalURL, maxBytes)
}

// readDirectImageResponse 校验直连下载的响应：重定向明确拒绝并提示用直链，
// 其它非 2xx 报状态码；大小/MIME 校验与出口下载完全一致（readImageResponse）。
func readDirectImageResponse(response *http.Response, source *url.URL, maxBytes int64) (provider.ImageInput, error) {
	if response.StatusCode >= http.StatusMultipleChoices && response.StatusCode < http.StatusBadRequest {
		return provider.ImageInput{}, fmt.Errorf("%w: 图片地址返回 %d 重定向，已按 SSRF 策略拒绝；请提供不经过重定向的直链 URL", errInvalidChatImage, response.StatusCode)
	}
	if response.StatusCode < 200 || response.StatusCode >= 300 {
		return provider.ImageInput{}, fmt.Errorf("%w: 下载地址返回 %d", errInvalidChatImage, response.StatusCode)
	}
	return readImageResponse(response.Body, response.ContentLength, response.Header.Get("Content-Type"), source, maxBytes)
}

// ownMediaPaths 返回本服务公开媒体资源在 URL 里的路径前缀（已并入 publicBaseURL 的路径部分）。
func (a *Adapter) ownMediaPaths() (base *url.URL, imagePrefix string) {
	baseURL := strings.TrimSpace(a.config().PublicBaseURL)
	if baseURL == "" {
		return nil, ""
	}
	parsed, err := url.Parse(baseURL)
	if err != nil || parsed.Scheme == "" || parsed.Host == "" {
		return nil, ""
	}
	basePath := strings.TrimRight(parsed.Path, "/")
	return parsed, basePath + "/v1/media/images/"
}

// ownMediaImageAssetID 判定 rawURL 是否指向本服务自己发布的图库图片
// （<publicBaseURL>/v1/media/images/<assetID>），返回 assetID。
// 命中后应优先读本地媒体存储，避免为了拿回自己刚生成的图片再走一次 CDN 网络。
func (a *Adapter) ownMediaImageAssetID(rawURL string) (string, bool) {
	base, imagePrefix := a.ownMediaPaths()
	if base == nil || imagePrefix == "" {
		return "", false
	}
	target, err := url.Parse(strings.TrimSpace(rawURL))
	if err != nil || !sameURLOrigin(base, target) {
		return "", false
	}
	assetID, ok := strings.CutPrefix(target.Path, imagePrefix)
	if !ok {
		return "", false
	}
	assetID = strings.TrimSpace(assetID)
	if assetID == "" || strings.Contains(assetID, "/") {
		return "", false
	}
	return assetID, true
}

// sameURLOrigin 比较 scheme/host/effective port，忽略大小写与默认端口写法差异。
func sameURLOrigin(left, right *url.URL) bool {
	if left == nil || right == nil {
		return false
	}
	if !strings.EqualFold(left.Scheme, right.Scheme) {
		return false
	}
	if !strings.EqualFold(left.Hostname(), right.Hostname()) {
		return false
	}
	return effectiveURLPort(left) == effectiveURLPort(right)
}

func effectiveURLPort(value *url.URL) string {
	if port := value.Port(); port != "" {
		return port
	}
	switch strings.ToLower(value.Scheme) {
	case "http":
		return "80"
	case "https":
		return "443"
	default:
		return ""
	}
}

// loadLocalMediaImage 优先从本地媒体存储读取自有媒体 URL 对应的图片。
// 返回 ok=false 表示本地没有这张图（例如来自其它实例、已被清理），
// 调用方应退化为直连下载。
func (a *Adapter) loadLocalMediaImage(ctx context.Context, assetID string, maxBytes int64) (provider.ImageInput, bool, error) {
	reader, ok := a.assets.(localImageAssetReader)
	if !ok {
		return provider.ImageInput{}, false, nil
	}
	asset, body, err := reader.OpenImage(ctx, assetID)
	if err != nil {
		return provider.ImageInput{}, false, nil
	}
	defer body.Close()
	if asset.SizeBytes > maxBytes {
		return provider.ImageInput{}, false, nil
	}
	raw, err := io.ReadAll(io.LimitReader(body, maxBytes+1))
	if err != nil || int64(len(raw)) > maxBytes {
		return provider.ImageInput{}, false, nil
	}
	mimeType, err := validatedImageMIME(raw, asset.MIMEType)
	if err != nil {
		return provider.ImageInput{}, true, err
	}
	return provider.ImageInput{Filename: "image" + imageExtension(mimeType), MIMEType: mimeType, Data: raw}, true, nil
}
