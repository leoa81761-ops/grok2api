package web

import (
	"bufio"
	"context"
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
	"net/url"
	"regexp"
	"sort"
	"strings"
	"time"
	"unicode/utf8"

	"github.com/chenyme/grok2api/backend/internal/domain/account"
	domainegress "github.com/chenyme/grok2api/backend/internal/domain/egress"
	settingsdomain "github.com/chenyme/grok2api/backend/internal/domain/settings"
	infraegress "github.com/chenyme/grok2api/backend/internal/infra/egress"
	"github.com/chenyme/grok2api/backend/internal/infra/provider"
)

type webMediaUpstreamError struct {
	status              int
	summary             string
	bodyBytes           int
	bodyTruncated       bool
	bodyPrefixSHA256    string
	bodyKind            string
	cloudflareChallenge bool
}

func (e *webMediaUpstreamError) Error() string {
	if e == nil {
		return ""
	}
	return e.summary
}

func (e *webMediaUpstreamError) HTTPStatusCode() int {
	if e == nil {
		return 0
	}
	return e.status
}

// isClearanceRefreshableMediaError distinguishes browser-session challenges
// from structured upstream policy responses such as content moderation. Empty
// and HTML 403 bodies are the forms returned by the media endpoints when the
// request is rejected before the application response is built.
func isClearanceRefreshableMediaError(e *webMediaUpstreamError) bool {
	if e == nil || e.status != http.StatusForbidden {
		return false
	}
	return e.cloudflareChallenge || e.bodyKind == "empty" || e.bodyKind == "html"
}

// isStatsigRefreshableMediaError identifies application-layer rejections that
// tell the browser to reload its page state. Grok uses code 7 for this anti-bot
// response, but the same code can also wrap a definitive account block; blocked
// credentials must remain terminal and must not be replayed with a fresh signature.
func isStatsigRefreshableMediaError(e *webMediaUpstreamError, body []byte) bool {
	if e == nil || e.status != http.StatusForbidden || e.bodyKind != "json" || provider.IsDefinitiveAccountBlockBody(body) {
		return false
	}
	code, message, structured := extractWebMediaUpstreamErrorFields(body)
	if !structured {
		return false
	}
	normalized := strings.ToLower(message)
	return code == "7" || strings.Contains(normalized, "anti-bot") ||
		strings.Contains(normalized, "page is out of date") || strings.Contains(normalized, "reload to continue")
}

func (e *webMediaUpstreamError) providerResponse() *provider.Response {
	if e == nil {
		return nil
	}
	code := "upstream_forbidden"
	if e.status != http.StatusForbidden {
		code = "upstream_unavailable"
	}
	return jsonProviderResponse(e.status, map[string]any{"error": map[string]any{
		"message": e.summary,
		"type":    "upstream_error",
		"code":    code,
	}})
}

const (
	webMediaDiagnosticBodyLimit    = 64 << 10
	webMediaDiagnosticSummaryLimit = 256
	webMediaDiagnosticFieldLimit   = 160
)

var (
	webMediaAuthorizationPattern = regexp.MustCompile(`(?i)\b(bearer|basic)\s+[A-Za-z0-9._~+/=-]+`)
	webMediaCookiePattern        = regexp.MustCompile(`(?i)\b(cookie|set-cookie)\b\s*[:=]\s*[^\r\n]+`)
	webMediaSecretPattern        = regexp.MustCompile(`(?i)(["']?(?:authorization|proxy-authorization|x-api-key|api[_-]?key|access[_-]?token|refresh[_-]?token|id[_-]?token|upload[_-]?url|cookie|sso|session[_-]?id)["']?\s*[:=]\s*["']?)[^"'\s,;}]+`)
	webMediaJWTPattern           = regexp.MustCompile(`\beyJ[A-Za-z0-9_-]{12,}\.[A-Za-z0-9_-]{12,}(?:\.[A-Za-z0-9_-]{12,})?\b`)
	webMediaEmailPattern         = regexp.MustCompile(`(?i)\b[A-Z0-9._%+-]+@[A-Z0-9.-]+\.[A-Z]{2,}\b`)
	webMediaURLPattern           = regexp.MustCompile(`https?://[^\s"'<>]+`)
	webMediaLongTokenPattern     = regexp.MustCompile(`[A-Za-z0-9+/=_-]{256,}`)
)

// newWebMediaUpstreamError keeps the HTTP status while exposing only a
// bounded, redacted summary through the error. Structured logs retain only
// body metadata and a prefix hash, never the upstream response body itself.
func newWebMediaUpstreamError(status int, body []byte, truncated bool) *webMediaUpstreamError {
	digest := sha256.Sum256(body)
	return &webMediaUpstreamError{
		status:              status,
		summary:             summarizeWebMediaUpstreamError(status, body, truncated),
		bodyBytes:           len(body),
		bodyTruncated:       truncated,
		bodyPrefixSHA256:    fmt.Sprintf("%x", digest),
		bodyKind:            classifyWebMediaDiagnosticBody(body),
		cloudflareChallenge: isCloudflareChallengeBody(body),
	}
}

func classifyWebMediaDiagnosticBody(body []byte) string {
	if !utf8.Valid(body) {
		return "binary"
	}
	trimmed := strings.TrimSpace(string(body))
	if trimmed == "" {
		return "empty"
	}
	if json.Valid(body) {
		return "json"
	}
	lower := strings.ToLower(trimmed)
	if strings.HasPrefix(lower, "<!doctype html") || strings.HasPrefix(lower, "<html") {
		return "html"
	}
	for _, value := range trimmed {
		if value < 0x20 && value != '\t' && value != '\r' && value != '\n' {
			return "binary"
		}
	}
	return "text"
}

func isCloudflareChallengeBody(body []byte) bool {
	lower := strings.ToLower(string(body))
	return strings.Contains(lower, "just a moment") ||
		strings.Contains(lower, "challenge-platform") ||
		strings.Contains(lower, "__cf_chl") ||
		strings.Contains(lower, "cf-chl-")
}

func (a *Adapter) logWebMediaUpstreamRejection(stage string, response *http.Response, upstreamErr *webMediaUpstreamError) {
	if upstreamErr == nil {
		return
	}
	attributes := []any{
		"stage", stage,
		"status", upstreamErr.status,
		"body_bytes_captured", upstreamErr.bodyBytes,
		"body_truncated", upstreamErr.bodyTruncated,
		"body_prefix_sha256", upstreamErr.bodyPrefixSHA256,
		"body_kind", upstreamErr.bodyKind,
		"cloudflare_challenge", upstreamErr.cloudflareChallenge,
	}
	if response != nil {
		attributes = append(attributes,
			"content_type", safeWebMediaDiagnostic(response.Header.Get("Content-Type"), 128),
			"content_length", response.ContentLength,
			"content_encoding", safeWebMediaDiagnostic(response.Header.Get("Content-Encoding"), 64),
			"server", safeWebMediaDiagnostic(response.Header.Get("Server"), 128),
			"cf_ray", safeWebMediaDiagnostic(response.Header.Get("CF-Ray"), 128),
			"upstream_request_id", safeWebMediaDiagnostic(firstNonEmpty(response.Header.Get("X-Request-Id"), response.Header.Get("X-Xai-Request-Id")), 128),
		)
	}
	a.log().Warn("web_media_upstream_rejected", attributes...)
}

func summarizeWebMediaUpstreamError(status int, body []byte, truncated bool) string {
	code, message, structured := extractWebMediaUpstreamErrorFields(body)
	parts := []string{fmt.Sprintf("Grok Web 媒体上游返回 %d", status)}
	if code != "" {
		parts = append(parts, code)
	}
	if message != "" {
		parts = append(parts, message)
	} else if len(strings.TrimSpace(string(body))) == 0 {
		parts = append(parts, "<empty>")
	} else if truncated {
		parts = append(parts, "响应正文过长")
	} else if !structured {
		parts = append(parts, "响应正文不可解析")
	} else if code == "" {
		parts = append(parts, "未提供错误详情")
	}
	return boundWebMediaDiagnostic(strings.Join(parts, ": "), webMediaDiagnosticSummaryLimit)
}

func extractWebMediaUpstreamErrorFields(body []byte) (code, message string, structured bool) {
	var root map[string]any
	if err := json.Unmarshal(body, &root); err != nil {
		return "", "", false
	}
	structured = true
	if errorObject, ok := root["error"].(map[string]any); ok {
		code = firstWebMediaDiagnosticCode(errorObject, "code", "type", "error")
		message = firstString(errorObject, "message", "error", "detail")
	} else if errorText, ok := root["error"].(string); ok {
		message = errorText
	}
	if code == "" {
		code = firstWebMediaDiagnosticCode(root, "code", "error_code", "type")
	}
	if message == "" {
		message = firstString(root, "message", "error_message", "detail")
	}
	return safeWebMediaDiagnostic(code, 64), safeWebMediaDiagnostic(message, webMediaDiagnosticFieldLimit), true
}

func firstWebMediaDiagnosticCode(value map[string]any, keys ...string) string {
	if code := firstString(value, keys...); code != "" {
		return code
	}
	if code, ok := firstInt(value, keys...); ok {
		return fmt.Sprintf("%d", code)
	}
	return ""
}

func safeWebMediaDiagnostic(value string, limit int) string {
	value = strings.Join(strings.Fields(value), " ")
	value = webMediaCookiePattern.ReplaceAllString(value, "$1: [REDACTED]")
	value = webMediaAuthorizationPattern.ReplaceAllString(value, "$1 [REDACTED]")
	value = webMediaSecretPattern.ReplaceAllString(value, "$1[REDACTED]")
	value = webMediaJWTPattern.ReplaceAllString(value, "[REDACTED]")
	value = webMediaEmailPattern.ReplaceAllString(value, "[REDACTED_EMAIL]")
	value = webMediaURLPattern.ReplaceAllString(value, "[REDACTED_URL]")
	value = webMediaLongTokenPattern.ReplaceAllString(value, "[REDACTED_LONG_VALUE]")
	return boundWebMediaDiagnostic(value, limit)
}

func boundWebMediaDiagnostic(value string, limit int) string {
	if limit <= 0 || value == "" {
		return ""
	}
	runes := []rune(value)
	if len(runes) <= limit {
		return value
	}
	return string(runes[:limit])
}

func (a *Adapter) GenerateVideo(ctx context.Context, request provider.VideoRequest) (provider.VideoResult, error) {
	// 首/尾帧映射与上限见 videoFrameSources：0 张 -> 文生视频，1 张 -> 图生视频
	// （首帧），2 张 -> 首尾帧（referenceToVideo.firstFrameAsset/lastFrameAsset）。
	frames, err := videoFrameSources(request.ImageURL, request.ReferenceURLs)
	if err != nil {
		return provider.VideoResult{}, provider.WrapVideoStage(provider.VideoStagePrepare, 0, err)
	}
	cfg := a.config()
	if len(videoSegments(request.Duration)) == 0 {
		return provider.VideoResult{}, provider.WrapVideoStage(provider.VideoStagePrepare, 0, fmt.Errorf("duration 必须在 1 到 15 秒之间"))
	}
	token, err := a.cipher.Decrypt(request.Credential.EncryptedAccessToken)
	if err != nil {
		return provider.VideoResult{}, provider.WrapVideoStage(provider.VideoStagePrepare, 0, err)
	}
	lease, err := a.egress.AcquireCredential(ctx, domainegress.ScopeWeb, request.Credential)
	if err != nil {
		return provider.VideoResult{}, provider.WrapVideoStage(provider.VideoStagePrepare, 0, err)
	}
	defer lease.Release()
	seconds := applyFreeWebVideoDurationCap(request.Duration, cfg.FreeVideoDurationCap, request.Credential)
	segments := videoSegments(seconds)
	ratio := resolveAspectRatio(request.AspectRatio)
	resolution := request.Resolution
	if resolution == "" {
		resolution = "720p"
	}
	// 帧图片必须先上传成资产 id：conversations/new 只接受 imageToVideo.inputAssets /
	// referenceToVideo.firstFrameAsset|lastFrameAsset 里的 asset 引用，不接受图片 URL。
	assets, err := a.uploadVideoFrames(ctx, cfg, lease, token, frames)
	if err != nil {
		return provider.VideoResult{}, err
	}
	if len(assets) > 0 {
		a.log().Info("web_video_frame_input",
			"frame_count", len(assets), "duration", segments[0], "resolution", resolution)
	}
	payload := videoPayloadForFrames(request.Prompt, ratio, resolution, segments[0], assets)
	response, err := a.postJSON(ctx, cfg, lease, token, cfg.BaseURL+"/rest/app-chat/conversations/new", payload, time.Duration(cfg.VideoTimeoutSeconds)*time.Second)
	if err != nil {
		return provider.VideoResult{}, provider.WrapVideoStage(provider.VideoCreateFailureStage(err), 0, err)
	}
	result, _, parseErr := parseVideoStream(response, request.Progress)
	_ = response.Body.Close()
	if parseErr != nil {
		if upstreamErr, ok := parseErr.(*webMediaUpstreamError); ok {
			a.logWebMediaUpstreamRejection("video_generation", response, upstreamErr)
		}
		stage := provider.VideoStagePoll
		if response.StatusCode < http.StatusOK || response.StatusCode >= http.StatusMultipleChoices {
			stage = provider.VideoCreateFailureStage(parseErr)
		}
		return provider.VideoResult{}, provider.WrapVideoStage(stage, 0, parseErr)
	}
	if result.URL == "" {
		return provider.VideoResult{}, provider.WrapVideoStage(provider.VideoStagePoll, 0, fmt.Errorf("视频生成完成但没有返回内容 URL"))
	}
	return result, nil
}

// DownloadVideo retrieves a completed Grok asset through its source SSO
// session. Direct asset URLs are not public and must not be exposed as a
// substitute for this authenticated transfer.
func (a *Adapter) DownloadVideo(ctx context.Context, credential account.Credential, rawURL string) (io.ReadCloser, string, int64, error) {
	parsed, err := url.Parse(strings.TrimSpace(rawURL))
	if err != nil || parsed.Scheme != "https" || !trustedImageAssetHost(parsed.Hostname()) || parsed.User != nil {
		return nil, "", 0, fmt.Errorf("视频内容 URL 不受信任")
	}
	token, err := a.cipher.Decrypt(credential.EncryptedAccessToken)
	if err != nil {
		return nil, "", 0, err
	}
	// 视频生成与成品下载必须复用同一账号身份；否则 Resin 会为 WebAsset
	// 重新分配租约，账号级 Cloudflare clearance 也不会进入下载请求。
	lease, err := a.egress.AcquireCredential(ctx, domainegress.ScopeWebAsset, credential)
	if err != nil {
		return nil, "", 0, err
	}
	request, err := http.NewRequestWithContext(ctx, http.MethodGet, parsed.String(), nil)
	if err != nil {
		lease.Release()
		return nil, "", 0, err
	}
	request.Header = buildHeaders(token, lease, "")
	request.Header.Del("Content-Type")
	response, err := lease.Do(request)
	if err != nil {
		a.egress.FeedbackForScope(context.WithoutCancel(ctx), domainegress.ScopeWebAsset, lease.NodeID, 0, err)
		lease.Release()
		return nil, "", 0, err
	}
	if response.StatusCode < 200 || response.StatusCode >= 300 {
		_ = response.Body.Close()
		a.egress.FeedbackForScope(context.WithoutCancel(ctx), domainegress.ScopeWebAsset, lease.NodeID, response.StatusCode, nil)
		lease.Release()
		return nil, "", 0, fmt.Errorf("下载视频返回 %d", response.StatusCode)
	}
	contentType := strings.ToLower(strings.TrimSpace(strings.Split(response.Header.Get("Content-Type"), ";")[0]))
	if contentType == "" || contentType == "application/octet-stream" {
		contentType = "video/mp4"
	}
	if !strings.HasPrefix(contentType, "video/") {
		_ = response.Body.Close()
		lease.Release()
		return nil, "", 0, fmt.Errorf("上游视频 Content-Type 无效")
	}
	onFinished := func(readErr error, complete bool) {
		if readErr != nil {
			a.egress.FeedbackForScope(context.WithoutCancel(ctx), domainegress.ScopeWebAsset, lease.NodeID, 0, readErr)
		} else if complete {
			a.egress.FeedbackForScope(context.WithoutCancel(ctx), domainegress.ScopeWebAsset, lease.NodeID, response.StatusCode, nil)
		}
		lease.Release()
	}
	return provider.NewCompletionReadCloser(response.Body, onFinished), contentType, response.ContentLength, nil
}

func parseVideoStream(response *http.Response, progress func(int)) (provider.VideoResult, string, error) {
	if response.StatusCode < 200 || response.StatusCode >= 300 {
		body, _ := io.ReadAll(io.LimitReader(response.Body, webMediaDiagnosticBodyLimit+1))
		if response.StatusCode == http.StatusUnauthorized {
			return provider.VideoResult{}, "", provider.ErrUnauthorized
		}
		truncated := len(body) > webMediaDiagnosticBodyLimit
		if truncated {
			body = body[:webMediaDiagnosticBodyLimit]
		}
		return provider.VideoResult{}, "", newWebMediaUpstreamError(response.StatusCode, body, truncated)
	}
	var result provider.VideoResult
	var postID string
	handle := func(root map[string]any) (bool, error) {
		// TEMPORARY DEBUG (2026-10-24): dump upstream video stream shape so we can
		// tell a real generation event from a gallery attachment. Remove before PR.
		if debugRaw, ok := root["result"].(map[string]any); ok {
			if resp, ok := debugRaw["response"].(map[string]any); ok {
				keys := make([]string, 0, len(resp))
				for k := range resp {
					keys = append(keys, k)
				}
				sort.Strings(keys)
				log.Printf("[VIDEO-DEBUG] response keys=%v", keys)
				if user, ok := resp["userResponse"].(map[string]any); ok {
					raw, _ := json.Marshal(user)
					if len(raw) > 500 {
						raw = raw[:500]
					}
					log.Printf("[VIDEO-DEBUG] userResponse=%s", string(raw))
				}
				if action, ok := resp["queryAction"]; ok {
					raw, _ := json.Marshal(action)
					if len(raw) > 300 {
						raw = raw[:300]
					}
					log.Printf("[VIDEO-DEBUG] queryAction=%s", string(raw))
				}
				if stream, ok := resp["streamingVideoGenerationResponse"].(map[string]any); ok {
					raw, _ := json.Marshal(stream)
					if len(raw) > 400 {
						raw = raw[:400]
					}
					log.Printf("[VIDEO-DEBUG] stream=%s", string(raw))
				}
				if modelResp, ok := resp["modelResponse"].(map[string]any); ok {
					log.Printf("[VIDEO-DEBUG] modelResponse keys=%v", func() []string {
						out := make([]string, 0, len(modelResp))
						for k := range modelResp {
							out = append(out, k)
						}
						sort.Strings(out)
						return out
					}())
					if att, ok := modelResp["fileAttachments"].([]any); ok {
						log.Printf("[VIDEO-DEBUG] fileAttachments=%v", att)
					}
				}
			}
		}
		if errorValue, ok := root["error"].(map[string]any); ok {
			return false, webMediaStreamError(errorValue)
		}
		if errorValue := nestedMap(root, "result", "response", "error"); errorValue != nil {
			return false, webMediaStreamError(errorValue)
		}
		stream := nestedMap(root, "result", "response", "streamingVideoGenerationResponse")
		if stream != nil {
			if value, ok := numberAsInt(stream["progress"]); ok && progress != nil {
				progress(value)
			}
			if value, _ := stream["videoPostId"].(string); value != "" {
				postID = value
			} else if value, _ := stream["videoId"].(string); value != "" {
				postID = value
			}
			moderated, _ := stream["moderated"].(bool)
			if moderated {
				return false, nil
			}
			if setVideoResultURL(&result, firstString(stream, "videoUrl", "contentUrl", "contentURL", "assetUrl", "assetURL", "fileUri", "fileURL")) {
				return true, nil
			}
		}
		for _, attachment := range videoFileAttachments(root) {
			if setVideoResultURL(&result, attachment) {
				return true, nil
			}
		}
		return false, nil
	}

	reader := bufio.NewReader(response.Body)
	prefix, _ := reader.Peek(64)
	trimmedPrefix := strings.TrimSpace(string(prefix))
	var err error
	if strings.HasPrefix(trimmedPrefix, "data:") || strings.HasPrefix(trimmedPrefix, "event:") {
		err = consumeVideoSSE(reader, handle)
	} else {
		err = consumeVideoJSON(reader, handle)
	}
	if err != nil {
		return provider.VideoResult{}, "", err
	}
	return result, postID, nil
}

func webMediaStreamError(value map[string]any) error {
	message := safeWebMediaDiagnostic(firstString(value, "message", "error", "detail"), webMediaDiagnosticFieldLimit)
	if message == "" {
		message = "未提供错误详情"
	}
	return fmt.Errorf("视频上游错误: %s", message)
}

func videoFileAttachments(root map[string]any) []string {
	modelResponse := nestedMap(root, "result", "response", "modelResponse")
	if modelResponse == nil {
		return nil
	}
	values, _ := modelResponse["fileAttachments"].([]any)
	attachments := make([]string, 0, len(values))
	for _, value := range values {
		if attachment, _ := value.(string); attachment != "" {
			attachments = append(attachments, attachment)
		}
	}
	return attachments
}

func setVideoResultURL(result *provider.VideoResult, value string) bool {
	value = strings.TrimSpace(value)
	if value == "" {
		return false
	}
	lower := strings.ToLower(value)
	if !strings.HasSuffix(strings.SplitN(lower, "?", 2)[0], ".mp4") && !strings.Contains(lower, "/content") {
		return false
	}
	result.URL = absoluteAssetURL(value)
	result.ContentType = "video/mp4"
	return true
}

func consumeVideoSSE(reader io.Reader, handle func(map[string]any) (bool, error)) error {
	scanner := bufio.NewScanner(reader)
	scanner.Buffer(make([]byte, 64<<10), 8<<20)
	for scanner.Scan() {
		line := strings.TrimSpace(scanner.Text())
		if strings.HasPrefix(line, "data:") {
			line = strings.TrimSpace(strings.TrimPrefix(line, "data:"))
		}
		if line == "" || line == "[DONE]" || !strings.HasPrefix(line, "{") {
			continue
		}
		var root map[string]any
		if json.Unmarshal([]byte(line), &root) != nil {
			continue
		}
		complete, err := handle(root)
		if err != nil {
			return err
		}
		if complete {
			return nil
		}
	}
	return scanner.Err()
}

func consumeVideoJSON(reader io.Reader, handle func(map[string]any) (bool, error)) error {
	decoder := json.NewDecoder(io.LimitReader(reader, 64<<20))
	for {
		var root map[string]any
		if err := decoder.Decode(&root); err != nil {
			if err == io.EOF {
				return nil
			}
			return fmt.Errorf("解析视频上游流: %w", err)
		}
		complete, err := handle(root)
		if err != nil {
			return err
		}
		if complete {
			return nil
		}
	}
}

func nestedMap(value map[string]any, keys ...string) map[string]any {
	current := value
	for _, key := range keys {
		next, ok := current[key].(map[string]any)
		if !ok {
			return nil
		}
		current = next
	}
	return current
}

func videoSegments(seconds int) []int {
	if seconds < 1 || seconds > 15 {
		return nil
	}
	return []int{seconds}
}

func normalizeFreeVideoDurationCap(value int) int {
	return settingsdomain.NormalizeWebFreeVideoDurationCap(value)
}

func shouldCapWebVideoDuration(credential account.Credential) bool {
	return credential.WebTier == account.WebTierBasic
}

// applyFreeWebVideoDurationCap clamps duration for free-tier Web accounts so
// upstream 429s from >6s (or the configured cap) do not trigger useless account rotation.
func applyFreeWebVideoDurationCap(seconds, cap int, credential account.Credential) int {
	if !shouldCapWebVideoDuration(credential) {
		return seconds
	}
	cap = normalizeFreeVideoDurationCap(cap)
	if seconds > cap {
		return cap
	}
	return seconds
}

// videoFrameSources maps the public video request fields onto at most two frame
// images, returned in [firstFrame, lastFrame] order.
//
// 映射约定（与 ops47/frame-video-impl.md 一致）：
//   - 0 张图：文生视频（textToVideo）
//   - 1 张图：图生视频（imageToVideo），该图是首帧，上游以 useFirstFrame 标记
//   - 2 张图：首尾帧（referenceToVideo），firstFrameAsset + lastFrameAsset
//
// ImageURL 是官方 `image` 字段，始终代表首帧；此时 ReferenceURLs[0] 是尾帧。
// 当前公开入口（/v1/videos/generations）仍按官方契约禁止 image 与
// reference_images 同时出现，因此经 HTTP 到达这里的首尾帧请求实际形如
// reference_images=[首帧, 尾帧]：Web 通道的 referenceToVideo.inputAssets 固定为
// 空，没有"纯风格参考"槽位，所以两张参考图只能解释成首帧和尾帧。
// 超过两张图在上游没有可表达的请求形状，直接拒绝而不是静默丢图。
func videoFrameSources(imageURL string, referenceURLs []string) ([]string, error) {
	frames := make([]string, 0, 2)
	if value := strings.TrimSpace(imageURL); value != "" {
		frames = append(frames, value)
	}
	for _, raw := range referenceURLs {
		if value := strings.TrimSpace(raw); value != "" {
			frames = append(frames, value)
		}
	}
	if len(frames) > 2 {
		return nil, fmt.Errorf("Grok Web 首尾帧生视频最多支持 2 张图片（首帧 + 尾帧），当前 %d 张", len(frames))
	}
	return frames, nil
}

// uploadVideoFrames turns the ordered frame sources into Imagine asset ids.
// 复用既有对话附件链路（loadChatImage + prepareChatAttachments），不新增上传协议。
// 逐张上传而不是合并成一次调用是有意的：prepareChatAttachments 会按内容去重，
// 首帧和尾帧是同一张图时合并调用会把两张塌成一张、破坏首尾帧请求。
func (a *Adapter) uploadVideoFrames(ctx context.Context, cfg Config, lease *infraegress.Lease, token string, frames []string) ([]string, error) {
	if len(frames) == 0 {
		return nil, nil
	}
	assets := make([]string, 0, len(frames))
	for index, frame := range frames {
		uploaded, err := a.prepareChatAttachments(ctx, cfg, lease, token, []chatAttachmentInput{{Source: frame, Image: true}})
		if err != nil {
			// 上游明确拒绝（4xx/5xx 带 HTTP 状态）沿用 create 阶段语义，让 gateway
			// 的 403 refresh / 429 换号逻辑照常生效；本地图片校验或下载失败属于
			// prepare，重试其它账号没有意义。
			stage := provider.VideoStagePrepare
			if _, hasStatus := provider.ErrorHTTPStatus(err); hasStatus {
				stage = provider.VideoCreateFailureStage(err)
			}
			return nil, provider.WrapVideoStage(stage, 0, fmt.Errorf("上传视频第 %d 帧图片: %w", index+1, err))
		}
		if len(uploaded) != 1 || strings.TrimSpace(uploaded[0]) == "" {
			return nil, provider.WrapVideoStage(provider.VideoStagePrepare, 0, fmt.Errorf("上传视频第 %d 帧图片成功但上游未返回可用资产标识", index+1))
		}
		assets = append(assets, uploaded[0])
	}
	return assets, nil
}

// videoConversationPayload mirrors the current Grok Imagine browser request.
// In particular, the generation parameters belong in mediaGenInput rather than
// the legacy modelConfigOverride map. textToVideo / imageToVideo /
// referenceToVideo share this envelope and differ only in mediaGenInput.
func videoConversationPayload(prompt string, mediaGenInput map[string]any) map[string]any {
	return map[string]any{
		"modelName":            "imagine-video-gen",
		"message":              videoConversationMessage(prompt),
		"enableImageStreaming": true,
		"enableSideBySide":     true,
		"sendFinalMetadata":    true,
		"responseMetadata": map[string]any{
			"experiments": []any{},
			"modelConfigOverride": map[string]any{
				"modelMap": map[string]any{},
			},
		},
		"mediaGenInput": mediaGenInput,
		"kind":          "CONVERSATION_KIND_IMAGINE",
	}
}

// videoConversationMessage reproduces the browser "<prompt> --mode=custom"
// message. 图生视频允许省略 prompt，此时不能留下前导空格。
func videoConversationMessage(prompt string) string {
	if strings.TrimSpace(prompt) == "" {
		return "--mode=custom"
	}
	return prompt + " --mode=custom"
}

// videoCreatePayload 构造纯文生视频请求（保持既有抓包形状不变）。
func videoCreatePayload(prompt, ratio, resolution string, seconds int) map[string]any {
	return videoConversationPayload(prompt, map[string]any{
		"textToVideo": map[string]any{
			"prompt":         prompt,
			"aspectRatio":    ratio,
			"duration":       seconds,
			"resolutionName": resolution,
		},
	})
}

// imageToVideoPayload 构造单首帧请求。字段名与 grok.com 前端一致：
// inputAssets 只放一张上传后的 asset id。前端默认省略 useFirstFrame（只有在
// useFirstFrame === false 时才显式下发），这里显式写 true：语义相同，且在
// 服务端把缺省值当 false 时也能保证首帧生效。
func imageToVideoPayload(prompt, ratio, resolution string, seconds int, assetID string) map[string]any {
	imageToVideo := map[string]any{
		"inputAssets":    []any{assetID},
		"aspectRatio":    ratio,
		"duration":       seconds,
		"resolutionName": resolution,
		"useFirstFrame":  true,
	}
	// 图生视频的 prompt 在上游是可选字段。
	if strings.TrimSpace(prompt) != "" {
		imageToVideo["prompt"] = prompt
	}
	return videoConversationPayload(prompt, map[string]any{"imageToVideo": imageToVideo})
}

// referenceToVideoPayload 构造首尾帧请求，字段名精确对齐 grok.com 前端
// buildReferenceToVideoInput：firstFrameAsset / lastFrameAsset 是图片上传返回的
// asset id；inputAssets 固定为空数组（Web 视频没有风格参考槽位）。
func referenceToVideoPayload(prompt, ratio, resolution string, seconds int, firstAssetID, lastAssetID string) map[string]any {
	return videoConversationPayload(prompt, map[string]any{
		"referenceToVideo": map[string]any{
			"prompt":          prompt,
			"inputAssets":     []any{},
			"aspectRatio":     ratio,
			"duration":        seconds,
			"resolutionName":  resolution,
			"firstFrameAsset": firstAssetID,
			"lastFrameAsset":  lastAssetID,
		},
	})
}

// videoPayloadForFrames 依据上传成功的帧数量选择 mediaGenInput 变体。
func videoPayloadForFrames(prompt, ratio, resolution string, seconds int, assets []string) map[string]any {
	switch len(assets) {
	case 0:
		return videoCreatePayload(prompt, ratio, resolution, seconds)
	case 1:
		return imageToVideoPayload(prompt, ratio, resolution, seconds, assets[0])
	default:
		return referenceToVideoPayload(prompt, ratio, resolution, seconds, assets[0], assets[1])
	}
}
