package web

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"slices"
	"sync/atomic"
	"testing"

	"github.com/chenyme/grok2api/backend/internal/domain/account"
	infraegress "github.com/chenyme/grok2api/backend/internal/infra/egress"
	"github.com/chenyme/grok2api/backend/internal/infra/provider"
	"github.com/chenyme/grok2api/backend/internal/infra/security"
)

// 1x1 PNG，loadChatImage 接受 data URI，这里用来避免测试里真的去下载外部图片。
const testFrameDataURI = "data:image/png;base64,iVBORw0KGgoAAAANSUhEUgAAAAEAAAABCAQAAAC1HAwCAAAAC0lEQVR42mNk+A8AAQUBAScY42YAAAAASUVORK5CYII="

const testVideoStreamFrame = `data: {"result":{"response":{"streamingVideoGenerationResponse":{"progress":100,"videoPostId":"post_1","videoUrl":"/videos/final.mp4"}}}}` + "\n\n"

func TestVideoFrameSourcesMapping(t *testing.T) {
	tests := []struct {
		name    string
		image   string
		refs    []string
		want    []string
		wantErr bool
	}{
		{name: "text only"},
		{name: "image is first frame", image: "https://example.com/first.png", want: []string{"https://example.com/first.png"}},
		{name: "single reference is first frame", refs: []string{"https://example.com/only.png"}, want: []string{"https://example.com/only.png"}},
		{name: "image plus reference is first and last", image: "https://example.com/first.png", refs: []string{"https://example.com/last.png"}, want: []string{"https://example.com/first.png", "https://example.com/last.png"}},
		{name: "two references are first and last", refs: []string{"https://example.com/first.png", "https://example.com/last.png"}, want: []string{"https://example.com/first.png", "https://example.com/last.png"}},
		{name: "empty slots are dropped", image: "  ", refs: []string{"", " https://example.com/last.png "}, want: []string{"https://example.com/last.png"}},
		{name: "three references rejected", refs: []string{"a", "b", "c"}, wantErr: true},
		{name: "image plus two references rejected", image: "a", refs: []string{"b", "c"}, wantErr: true},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			got, err := videoFrameSources(test.image, test.refs)
			if test.wantErr {
				if err == nil {
					t.Fatalf("frames = %#v, want error", got)
				}
				return
			}
			if err != nil {
				t.Fatal(err)
			}
			if !slices.Equal(got, test.want) {
				t.Fatalf("frames = %#v, want %#v", got, test.want)
			}
		})
	}
}

func TestVideoPayloadShapesMatchFrontendMediaGenInput(t *testing.T) {
	tests := []struct {
		name    string
		payload map[string]any
		want    string
	}{
		{
			name:    "text to video keeps captured shape",
			payload: videoPayloadForFrames("雨后天晴！", "9:16", "480p", 6, nil),
			want:    `{"enableImageStreaming":true,"enableSideBySide":true,"kind":"CONVERSATION_KIND_IMAGINE","mediaGenInput":{"textToVideo":{"aspectRatio":"9:16","duration":6,"prompt":"雨后天晴！","resolutionName":"480p"}},"message":"雨后天晴！ --mode=t2v","modelName":"imagine-video-gen","responseMetadata":{"experiments":[],"modelConfigOverride":{"modelMap":{}}},"sendFinalMetadata":true}`,
		},
		{
			name:    "single frame uses imageToVideo with useFirstFrame",
			payload: videoPayloadForFrames("镜头推进", "16:9", "480p", 6, []string{"asset-first"}),
			want:    `{"enableImageStreaming":true,"enableSideBySide":true,"kind":"CONVERSATION_KIND_IMAGINE","mediaGenInput":{"imageToVideo":{"aspectRatio":"16:9","duration":6,"inputAssets":["asset-first"],"prompt":"镜头推进","resolutionName":"480p","useFirstFrame":true}},"message":"镜头推进 --mode=i2v","modelName":"imagine-video-gen","responseMetadata":{"experiments":[],"modelConfigOverride":{"modelMap":{}}},"sendFinalMetadata":true}`,
		},
		{
			name:    "first and last frame use referenceToVideo asset fields",
			payload: videoPayloadForFrames("从白天到黑夜", "16:9", "480p", 6, []string{"asset-first", "asset-last"}),
			want:    `{"enableImageStreaming":true,"enableSideBySide":true,"kind":"CONVERSATION_KIND_IMAGINE","mediaGenInput":{"referenceToVideo":{"aspectRatio":"16:9","duration":6,"firstFrameAsset":"asset-first","inputAssets":[],"lastFrameAsset":"asset-last","prompt":"从白天到黑夜","resolutionName":"480p"}},"message":"从白天到黑夜 --mode=r2v","modelName":"imagine-video-gen","responseMetadata":{"experiments":[],"modelConfigOverride":{"modelMap":{}}},"sendFinalMetadata":true}`,
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			raw, err := json.Marshal(test.payload)
			if err != nil {
				t.Fatal(err)
			}
			if string(raw) != test.want {
				t.Fatalf("payload = %s\nwant      %s", raw, test.want)
			}
		})
	}
}

func TestImageToVideoPayloadOmitsEmptyPrompt(t *testing.T) {
	payload := imageToVideoPayload("", "1:1", "480p", 6, "asset-first")
	message := payload["message"]
	if message != "--mode=i2v" {
		t.Fatalf("message = %#v, want --mode=i2v", message)
	}
	imageToVideo, _ := payload["mediaGenInput"].(map[string]any)["imageToVideo"].(map[string]any)
	if _, exists := imageToVideo["prompt"]; exists {
		t.Fatalf("empty prompt leaked into imageToVideo: %#v", imageToVideo)
	}
}

// frameVideoServer 模拟上传与视频对话两个端点，按上传顺序发放 asset id。
func frameVideoServer(t *testing.T, uploads, generations *atomic.Int32, captured *[]byte) *httptest.Server {
	t.Helper()
	return httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		switch request.URL.Path {
		case "/http/upload-file-v2/direct":
			call := uploads.Add(1)
			if _, _, err := request.FormFile("file"); err != nil {
				t.Errorf("upload file part: %v", err)
			}
			writer.Header().Set("Content-Type", "application/json")
			_, _ = io.WriteString(writer, `{"fileMetadata":{"fileMetadataId":"asset-`+string(rune('0'+call))+`"}}`)
		case "/rest/app-chat/conversations/new":
			generations.Add(1)
			if captured != nil {
				body, err := io.ReadAll(request.Body)
				if err != nil {
					t.Errorf("read video payload: %v", err)
				}
				*captured = body
			}
			writer.Header().Set("Content-Type", "text/event-stream")
			_, _ = io.WriteString(writer, testVideoStreamFrame)
		default:
			http.NotFound(writer, request)
		}
	}))
}

func newFrameVideoAdapter(t *testing.T, baseURL string) (*Adapter, account.Credential) {
	t.Helper()
	cipher, err := security.NewCipher(base64.StdEncoding.EncodeToString(make([]byte, 32)))
	if err != nil {
		t.Fatal(err)
	}
	encryptedToken, err := cipher.Encrypt("test-sso")
	if err != nil {
		t.Fatal(err)
	}
	adapter := NewAdapter(Config{
		BaseURL: baseURL, StatsigMode: "manual", StatsigManualValue: "test",
		VideoTimeoutSeconds: 5, FreeVideoDurationCap: 6, MaxInputImageBytes: 1 << 20,
	}, infraegress.NewManager(egressRepositoryStub{}, cipher), cipher, nil, nil)
	return adapter, account.Credential{
		ID: 1, Provider: account.ProviderWeb, WebTier: account.WebTierBasic,
		EncryptedAccessToken: encryptedToken,
	}
}

func TestGenerateVideoFirstLastFrameUploadsBothFramesAndSendsReferenceToVideo(t *testing.T) {
	var uploads, generations atomic.Int32
	var captured []byte
	server := frameVideoServer(t, &uploads, &generations, &captured)
	defer server.Close()
	adapter, credential := newFrameVideoAdapter(t, server.URL)

	// 首尾帧用同一个 data URI：上传必须按帧逐一进行，不能因为内容去重塌成一张。
	result, err := adapter.GenerateVideo(context.Background(), provider.VideoRequest{
		Credential:    credential,
		Prompt:        "从白天到黑夜",
		Duration:      10, // 免费档应被压到 6 秒
		AspectRatio:   "16:9",
		Resolution:    "480p",
		ReferenceURLs: []string{testFrameDataURI, testFrameDataURI},
	})
	if err != nil {
		t.Fatalf("GenerateVideo: %v", err)
	}
	if result.URL != "https://assets.grok.com/videos/final.mp4" {
		t.Fatalf("result = %#v", result)
	}
	if uploads.Load() != 2 || generations.Load() != 1 {
		t.Fatalf("uploads/generations = %d/%d, want 2/1", uploads.Load(), generations.Load())
	}

	var payload map[string]any
	if err := json.Unmarshal(captured, &payload); err != nil {
		t.Fatalf("decode payload %s: %v", captured, err)
	}
	if payload["modelName"] != "imagine-video-gen" || payload["message"] != "从白天到黑夜 --mode=r2v" {
		t.Fatalf("payload = %#v", payload)
	}
	mediaGenInput, _ := payload["mediaGenInput"].(map[string]any)
	referenceToVideo, ok := mediaGenInput["referenceToVideo"].(map[string]any)
	if !ok {
		t.Fatalf("mediaGenInput = %#v", mediaGenInput)
	}
	if referenceToVideo["firstFrameAsset"] != "asset-1" || referenceToVideo["lastFrameAsset"] != "asset-2" {
		t.Fatalf("frame assets = %#v", referenceToVideo)
	}
	if assets, ok := referenceToVideo["inputAssets"].([]any); !ok || len(assets) != 0 {
		t.Fatalf("referenceToVideo.inputAssets = %#v, want empty array", referenceToVideo["inputAssets"])
	}
	if referenceToVideo["duration"] != float64(6) || referenceToVideo["resolutionName"] != "480p" ||
		referenceToVideo["aspectRatio"] != "16:9" || referenceToVideo["prompt"] != "从白天到黑夜" {
		t.Fatalf("referenceToVideo = %#v", referenceToVideo)
	}
}

func TestGenerateVideoSingleFrameUsesImageToVideo(t *testing.T) {
	var uploads, generations atomic.Int32
	var captured []byte
	server := frameVideoServer(t, &uploads, &generations, &captured)
	defer server.Close()
	adapter, credential := newFrameVideoAdapter(t, server.URL)

	if _, err := adapter.GenerateVideo(context.Background(), provider.VideoRequest{
		Credential: credential,
		ImageURL:   testFrameDataURI,
		Duration:   6,
		Resolution: "480p",
	}); err != nil {
		t.Fatalf("GenerateVideo: %v", err)
	}
	if uploads.Load() != 1 || generations.Load() != 1 {
		t.Fatalf("uploads/generations = %d/%d, want 1/1", uploads.Load(), generations.Load())
	}
	var payload map[string]any
	if err := json.Unmarshal(captured, &payload); err != nil {
		t.Fatalf("decode payload %s: %v", captured, err)
	}
	mediaGenInput, _ := payload["mediaGenInput"].(map[string]any)
	imageToVideo, ok := mediaGenInput["imageToVideo"].(map[string]any)
	if !ok {
		t.Fatalf("mediaGenInput = %#v", mediaGenInput)
	}
	if imageToVideo["useFirstFrame"] != true {
		t.Fatalf("useFirstFrame = %#v, want true", imageToVideo["useFirstFrame"])
	}
	assets, _ := imageToVideo["inputAssets"].([]any)
	if len(assets) != 1 || assets[0] != "asset-1" {
		t.Fatalf("inputAssets = %#v", imageToVideo["inputAssets"])
	}
}

func TestGenerateVideoTextRequestUploadsNothing(t *testing.T) {
	var uploads, generations atomic.Int32
	server := frameVideoServer(t, &uploads, &generations, nil)
	defer server.Close()
	adapter, credential := newFrameVideoAdapter(t, server.URL)

	if _, err := adapter.GenerateVideo(context.Background(), provider.VideoRequest{
		Credential: credential,
		Prompt:     "雨后天晴",
		Duration:   6,
	}); err != nil {
		t.Fatalf("GenerateVideo: %v", err)
	}
	if uploads.Load() != 0 || generations.Load() != 1 {
		t.Fatalf("uploads/generations = %d/%d, want 0/1", uploads.Load(), generations.Load())
	}
}

func TestGenerateVideoRejectsMoreThanTwoFramesBeforeAnyUpstreamCall(t *testing.T) {
	var uploads, generations atomic.Int32
	server := frameVideoServer(t, &uploads, &generations, nil)
	defer server.Close()
	adapter, credential := newFrameVideoAdapter(t, server.URL)

	_, err := adapter.GenerateVideo(context.Background(), provider.VideoRequest{
		Credential:    credential,
		Prompt:        "too many",
		Duration:      6,
		ReferenceURLs: []string{testFrameDataURI, testFrameDataURI, testFrameDataURI},
	})
	if err == nil {
		t.Fatal("three frames were accepted")
	}
	var stageError *provider.VideoStageError
	if !errors.As(err, &stageError) || stageError.Stage != provider.VideoStagePrepare {
		t.Fatalf("error = %v, want prepare-stage validation failure", err)
	}
	if uploads.Load() != 0 || generations.Load() != 0 {
		t.Fatalf("uploads/generations = %d/%d, want 0/0", uploads.Load(), generations.Load())
	}
}
