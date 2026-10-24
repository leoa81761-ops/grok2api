package web

import (
	"bytes"
	"context"
	"encoding/base64"
	"errors"
	"io"
	"sync/atomic"
	"testing"

	"github.com/chenyme/grok2api/backend/internal/domain/account"
	mediadomain "github.com/chenyme/grok2api/backend/internal/domain/media"
	infraegress "github.com/chenyme/grok2api/backend/internal/infra/egress"
	"github.com/chenyme/grok2api/backend/internal/infra/provider"
	"github.com/chenyme/grok2api/backend/internal/infra/security"
)

// ownMediaAssetStore 是本地图库的假实现：既能返回本地图片，也能模拟图片不存在。
type ownMediaAssetStore struct {
	data    []byte
	mime    string
	openErr error
	opened  []string
}

func (s *ownMediaAssetStore) SaveImage(context.Context, []byte) (mediadomain.Asset, error) {
	return mediadomain.Asset{}, errors.New("unused")
}

func (s *ownMediaAssetStore) PublicImageURL(string) string { return "" }

func (s *ownMediaAssetStore) OpenImage(_ context.Context, id string) (mediadomain.Asset, io.ReadCloser, error) {
	s.opened = append(s.opened, id)
	if s.openErr != nil {
		return mediadomain.Asset{}, nil, s.openErr
	}
	return mediadomain.Asset{ID: id, Kind: "image", MIMEType: s.mime, SizeBytes: int64(len(s.data))},
		io.NopCloser(bytes.NewReader(s.data)), nil
}

// recordingOwnHostLoader 记录自有域名直连下载是否被调用，替代真实网络。
type recordingOwnHostLoader struct {
	calls  int
	rawURL string
	image  provider.ImageInput
	err    error
}

func (l *recordingOwnHostLoader) LoadOwnHostImage(_ context.Context, rawURL string, _ int64) (provider.ImageInput, error) {
	l.calls++
	l.rawURL = rawURL
	return l.image, l.err
}

func testFramePNG(t *testing.T) []byte {
	t.Helper()
	image, err := parseChatImageDataURI(testFrameDataURI, 1<<20)
	if err != nil {
		t.Fatal(err)
	}
	return image.Data
}

// newOwnMediaAdapter 构造注入了 ownHostImageLoader 桩的 adapter。自有媒体路径一旦
// 意外走真实网络/出口代理，桩的调用计数与失败会立刻暴露出来。
func newOwnMediaAdapter(t *testing.T, cfg Config, assets provider.ImageAssetStore) (*Adapter, *recordingOwnHostLoader, *security.Cipher) {
	t.Helper()
	cipher, err := security.NewCipher(base64.StdEncoding.EncodeToString(make([]byte, 32)))
	if err != nil {
		t.Fatal(err)
	}
	adapter := NewAdapter(cfg, infraegress.NewManager(egressRepositoryStub{}, cipher), cipher, nil, assets)
	loader := &recordingOwnHostLoader{image: provider.ImageInput{Filename: "remote.png", MIMEType: "image/png", Data: testFramePNG(t)}}
	adapter.ownHostImages = loader
	return adapter, loader, cipher
}

func ownMediaCredential(t *testing.T, cipher *security.Cipher) account.Credential {
	t.Helper()
	encrypted, err := cipher.Encrypt("test-sso")
	if err != nil {
		t.Fatal(err)
	}
	return account.Credential{
		ID: 1, Provider: account.ProviderWeb, WebTier: account.WebTierBasic,
		EncryptedAccessToken: encrypted,
	}
}

func TestOwnMediaImageAssetIDDetection(t *testing.T) {
	tests := []struct {
		name      string
		baseURL   string
		rawURL    string
		wantID    string
		wantMatch bool
	}{
		{name: "own media image", baseURL: "https://x.1so.org", rawURL: "https://x.1so.org/v1/media/images/img_abc", wantID: "img_abc", wantMatch: true},
		{name: "own base with path", baseURL: "https://api.example/base", rawURL: "https://api.example/base/v1/media/images/img_abc", wantID: "img_abc", wantMatch: true},
		{name: "own base path not repeated", baseURL: "https://api.example/base", rawURL: "https://api.example/v1/media/images/img_abc"},
		{name: "explicit default port", baseURL: "https://x.1so.org:443", rawURL: "https://x.1so.org/v1/media/images/img_abc", wantID: "img_abc", wantMatch: true},
		{name: "other host", baseURL: "https://x.1so.org", rawURL: "https://evil.example/v1/media/images/img_abc"},
		{name: "scheme mismatch", baseURL: "https://x.1so.org", rawURL: "http://x.1so.org/v1/media/images/img_abc"},
		{name: "video path is not an image asset", baseURL: "https://x.1so.org", rawURL: "https://x.1so.org/v1/media/videos/vid_abc"},
		{name: "empty asset id", baseURL: "https://x.1so.org", rawURL: "https://x.1so.org/v1/media/images/"},
		{name: "nested asset path", baseURL: "https://x.1so.org", rawURL: "https://x.1so.org/v1/media/images/img_a/extra"},
		{name: "missing base url", baseURL: "", rawURL: "https://x.1so.org/v1/media/images/img_abc"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			adapter, _, _ := newOwnMediaAdapter(t, Config{PublicBaseURL: test.baseURL}, &ownMediaAssetStore{})
			id, matched := adapter.ownMediaImageAssetID(test.rawURL)
			if matched != test.wantMatch || id != test.wantID {
				t.Fatalf("ownMediaImageAssetID(%q) = %q/%t, want %q/%t", test.rawURL, id, matched, test.wantID, test.wantMatch)
			}
		})
	}
}

func TestLoadChatImagePrefersLocalMediaAsset(t *testing.T) {
	png := testFramePNG(t)
	store := &ownMediaAssetStore{data: png, mime: "image/png"}
	adapter, loader, _ := newOwnMediaAdapter(t, Config{PublicBaseURL: "https://x.1so.org"}, store)

	// lease 为 nil：证明自有媒体命中本地存储时完全不触碰账号出口租约。
	image, err := adapter.loadChatImage(context.Background(), nil, "https://x.1so.org/v1/media/images/img_local", 1<<20)
	if err != nil {
		t.Fatalf("loadChatImage: %v", err)
	}
	if !bytes.Equal(image.Data, png) || image.MIMEType != "image/png" {
		t.Fatalf("image = %#v", image)
	}
	if len(store.opened) != 1 || store.opened[0] != "img_local" {
		t.Fatalf("opened = %#v, want [img_local]", store.opened)
	}
	if loader.calls != 0 {
		t.Fatalf("own-host direct loader calls = %d, want 0 (local store hit)", loader.calls)
	}
}

func TestLoadChatImageFallsBackToDirectForMissingLocalAsset(t *testing.T) {
	store := &ownMediaAssetStore{openErr: errors.New("asset not found")}
	adapter, loader, _ := newOwnMediaAdapter(t, Config{PublicBaseURL: "https://x.1so.org"}, store)

	image, err := adapter.loadChatImage(context.Background(), nil, "https://x.1so.org/v1/media/images/img_gone", 1<<20)
	if err != nil {
		t.Fatalf("loadChatImage: %v", err)
	}
	if !bytes.Equal(image.Data, loader.image.Data) {
		t.Fatalf("image = %#v", image)
	}
	if loader.calls != 1 || loader.rawURL != "https://x.1so.org/v1/media/images/img_gone" {
		t.Fatalf("loader = %#v, want one direct call for the original URL", loader)
	}
}

func TestLoadChatImageOwnHostNonMediaPathGoesDirect(t *testing.T) {
	store := &ownMediaAssetStore{data: testFramePNG(t), mime: "image/png"}
	adapter, loader, _ := newOwnMediaAdapter(t, Config{PublicBaseURL: "https://x.1so.org"}, store)

	if _, err := adapter.loadChatImage(context.Background(), nil, "https://x.1so.org/v1/media/videos/vid_1", 1<<20); err != nil {
		t.Fatalf("loadChatImage: %v", err)
	}
	if loader.calls != 1 {
		t.Fatalf("direct loader calls = %d, want 1", loader.calls)
	}
	if len(store.opened) != 0 {
		t.Fatalf("local store was used for a non-media-image path: %#v", store.opened)
	}
}

func TestLoadChatImageExternalURLKeepsEgressPath(t *testing.T) {
	adapter, loader, _ := newOwnMediaAdapter(t, Config{PublicBaseURL: "https://x.1so.org"}, &ownMediaAssetStore{})
	// 外部私网地址必须在进入 egress 之前就被现有 SSRF 校验拒绝。
	if _, err := adapter.loadChatImage(context.Background(), nil, "https://10.0.0.8/photo.png", 1<<20); err == nil {
		t.Fatal("private external image URL was accepted")
	}
	if loader.calls != 0 {
		t.Fatalf("own-host loader calls = %d, want 0 for external URL", loader.calls)
	}
}

func TestDirectOwnHostImageLoaderRetainsSSRFValidation(t *testing.T) {
	loader := directOwnHostImageLoader{}
	for _, rawURL := range []string{
		"https://127.0.0.1/v1/media/images/img_abc",
		"https://10.1.2.3/v1/media/images/img_abc",
		"http://x.1so.org/v1/media/images/img_abc",
		"https://x.1so.org:8443/v1/media/images/img_abc",
	} {
		if _, err := loader.LoadOwnHostImage(context.Background(), rawURL, 1<<20); err == nil {
			t.Fatalf("%s was accepted by the direct own-host loader", rawURL)
		}
	}
}

func TestGenerateVideoOwnMediaFrameSkipsNetworkDownload(t *testing.T) {
	png := testFramePNG(t)
	store := &ownMediaAssetStore{data: png, mime: "image/png"}
	adapter, loader, cipher := newOwnMediaAdapter(t, Config{
		PublicBaseURL: "https://x.1so.org", StatsigMode: "manual", StatsigManualValue: "test",
		VideoTimeoutSeconds: 5, FreeVideoDurationCap: 6, MaxInputImageBytes: 1 << 20,
	}, store)

	var uploads, generations atomic.Int32
	// 测试服务器没有任何 /v1/media 路由：如果帧图真的去下载就会 404 并失败。
	server := frameVideoServer(t, &uploads, &generations, nil)
	defer server.Close()
	adapter.UpdateConfig(Config{
		BaseURL: server.URL, PublicBaseURL: "https://x.1so.org", StatsigMode: "manual", StatsigManualValue: "test",
		VideoTimeoutSeconds: 5, FreeVideoDurationCap: 6, MaxInputImageBytes: 1 << 20,
	})

	result, err := adapter.GenerateVideo(context.Background(), provider.VideoRequest{
		Credential: ownMediaCredential(t, cipher),
		Prompt:     "transition",
		Duration:   6,
		ImageURL:   "https://x.1so.org/v1/media/images/img_frame",
	})
	if err != nil {
		t.Fatalf("GenerateVideo: %v", err)
	}
	if result.URL == "" {
		t.Fatalf("result = %#v", result)
	}
	if loader.calls != 0 {
		t.Fatalf("own-host direct loader calls = %d, want 0", loader.calls)
	}
	if uploads.Load() != 1 || generations.Load() != 1 {
		t.Fatalf("uploads/generations = %d/%d, want 1/1", uploads.Load(), generations.Load())
	}
}
