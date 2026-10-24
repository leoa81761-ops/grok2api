package web

import (
	"bytes"
	"context"
	"encoding/base64"
	"errors"
	"io"
	"net/http"
	"net/url"
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

// recordingImageLoader 记录直连下载是否被调用，替代真实网络（也就断言了不经过
// egress：egress 只在上传链路里使用）。
type recordingImageLoader struct {
	calls  int
	rawURL string
	image  provider.ImageInput
	err    error
}

func (l *recordingImageLoader) LoadImage(_ context.Context, rawURL string, _ int64) (provider.ImageInput, error) {
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

// newOwnMediaAdapter 构造注入了 imageDirectLoader 桩的 adapter。图片下载一旦意外
// 走真实网络/账号出口，桩的调用计数会立刻暴露出来。
func newOwnMediaAdapter(t *testing.T, cfg Config, assets provider.ImageAssetStore) (*Adapter, *recordingImageLoader, *security.Cipher) {
	t.Helper()
	cipher, err := security.NewCipher(base64.StdEncoding.EncodeToString(make([]byte, 32)))
	if err != nil {
		t.Fatal(err)
	}
	adapter := NewAdapter(cfg, infraegress.NewManager(egressRepositoryStub{}, cipher), cipher, nil, assets)
	loader := &recordingImageLoader{image: provider.ImageInput{Filename: "remote.png", MIMEType: "image/png", Data: testFramePNG(t)}}
	adapter.directImages = loader
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

func TestDefaultAdapterUsesDirectImageLoader(t *testing.T) {
	cipher, err := security.NewCipher(base64.StdEncoding.EncodeToString(make([]byte, 32)))
	if err != nil {
		t.Fatal(err)
	}
	adapter := NewAdapter(Config{}, infraegress.NewManager(egressRepositoryStub{}, cipher), cipher, nil, &ownMediaAssetStore{})
	if _, ok := adapter.directImages.(directImageLoader); !ok {
		t.Fatalf("default image loader = %T, want directImageLoader", adapter.directImages)
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
		t.Fatalf("direct loader calls = %d, want 0 (local store hit)", loader.calls)
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

// 外部图床 URL 也必须直连：不再经过账号出口代理（lease 传 nil 也不能 panic）。
func TestLoadChatImageExternalURLUsesDirectLoader(t *testing.T) {
	adapter, loader, _ := newOwnMediaAdapter(t, Config{PublicBaseURL: "https://x.1so.org"}, &ownMediaAssetStore{})
	image, err := adapter.loadChatImage(context.Background(), nil, "https://httpbin.org/image/png", 1<<20)
	if err != nil {
		t.Fatalf("loadChatImage: %v", err)
	}
	if !bytes.Equal(image.Data, loader.image.Data) {
		t.Fatalf("image = %#v", image)
	}
	if loader.calls != 1 || loader.rawURL != "https://httpbin.org/image/png" {
		t.Fatalf("loader = %#v, want exactly one direct call", loader)
	}
}

func TestLoadChatImageReportsDirectLoaderFailure(t *testing.T) {
	adapter, loader, _ := newOwnMediaAdapter(t, Config{}, &ownMediaAssetStore{})
	loader.err = errors.New("context deadline exceeded")
	if _, err := adapter.loadChatImage(context.Background(), nil, "https://httpbin.org/image/png", 1<<20); err == nil {
		t.Fatal("direct loader failure was swallowed")
	}
}

func TestDirectImageLoaderRetainsSSRFValidation(t *testing.T) {
	loader := directImageLoader{}
	for _, rawURL := range []string{
		"https://127.0.0.1/v1/media/images/img_abc",
		"https://10.1.2.3/photo.png",
		"http://httpbin.org/image/png",
		"https://httpbin.org:8443/image/png",
	} {
		if _, err := loader.LoadImage(context.Background(), rawURL, 1<<20); err == nil {
			t.Fatalf("%s was accepted by the direct image loader", rawURL)
		}
	}
}

func TestReadDirectImageResponseRejectsRedirects(t *testing.T) {
	source, err := url.Parse("https://picsum.photos/200")
	if err != nil {
		t.Fatal(err)
	}
	for _, status := range []int{http.StatusMovedPermanently, http.StatusFound, http.StatusTemporaryRedirect} {
		response := &http.Response{
			StatusCode: status, ContentLength: -1,
			Header: http.Header{"Content-Type": []string{"text/html"}},
			Body:   io.NopCloser(bytes.NewReader([]byte("redirect"))),
		}
		_, err := readDirectImageResponse(response, source, 1<<20)
		if !errors.Is(err, errInvalidChatImage) {
			t.Fatalf("status %d error = %v, want errInvalidChatImage", status, err)
		}
		if !bytes.Contains([]byte(err.Error()), []byte("重定向")) || !bytes.Contains([]byte(err.Error()), []byte("直链")) {
			t.Fatalf("status %d error message = %q, want redirect/direct-link hint", status, err)
		}
	}
}

func TestReadDirectImageResponseStatusAndBodyValidation(t *testing.T) {
	source, err := url.Parse("https://images.example.test/photo.png")
	if err != nil {
		t.Fatal(err)
	}
	notFound := &http.Response{StatusCode: http.StatusNotFound, ContentLength: -1, Header: http.Header{}, Body: io.NopCloser(bytes.NewReader(nil))}
	if _, err := readDirectImageResponse(notFound, source, 1<<20); !errors.Is(err, errInvalidChatImage) {
		t.Fatalf("404 error = %v, want errInvalidChatImage", err)
	}
	png := testFramePNG(t)
	ok := &http.Response{
		StatusCode: http.StatusOK, ContentLength: int64(len(png)),
		Header: http.Header{"Content-Type": []string{"image/png"}},
		Body:   io.NopCloser(bytes.NewReader(png)),
	}
	image, err := readDirectImageResponse(ok, source, 1<<20)
	if err != nil || image.MIMEType != "image/png" || image.Filename != "photo.png" {
		t.Fatalf("image = %#v err = %v", image, err)
	}
	oversized := &http.Response{
		StatusCode: http.StatusOK, ContentLength: 2 << 20,
		Header: http.Header{"Content-Type": []string{"image/png"}},
		Body:   io.NopCloser(bytes.NewReader(png)),
	}
	if _, err := readDirectImageResponse(oversized, source, 1<<20); !errors.Is(err, errInvalidChatImage) {
		t.Fatalf("oversized error = %v, want errInvalidChatImage", err)
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
		t.Fatalf("direct loader calls = %d, want 0", loader.calls)
	}
	if uploads.Load() != 1 || generations.Load() != 1 {
		t.Fatalf("uploads/generations = %d/%d, want 1/1", uploads.Load(), generations.Load())
	}
}

// 外部图床首尾帧：帧图下载走直连（注入的 loader），上传仍走 grok 链路。
func TestGenerateVideoExternalFrameUsesDirectLoader(t *testing.T) {
	adapter, loader, cipher := newOwnMediaAdapter(t, Config{
		StatsigMode: "manual", StatsigManualValue: "test",
		VideoTimeoutSeconds: 5, FreeVideoDurationCap: 6, MaxInputImageBytes: 1 << 20,
	}, &ownMediaAssetStore{})

	var uploads, generations atomic.Int32
	server := frameVideoServer(t, &uploads, &generations, nil)
	defer server.Close()
	adapter.UpdateConfig(Config{
		BaseURL: server.URL, StatsigMode: "manual", StatsigManualValue: "test",
		VideoTimeoutSeconds: 5, FreeVideoDurationCap: 6, MaxInputImageBytes: 1 << 20,
	})

	result, err := adapter.GenerateVideo(context.Background(), provider.VideoRequest{
		Credential:    ownMediaCredential(t, cipher),
		Prompt:        "transition",
		Duration:      6,
		ReferenceURLs: []string{"https://httpbin.org/image/png", "https://httpbin.org/image/jpeg"},
	})
	if err != nil {
		t.Fatalf("GenerateVideo: %v", err)
	}
	if result.URL == "" {
		t.Fatalf("result = %#v", result)
	}
	if loader.calls != 2 {
		t.Fatalf("direct loader calls = %d, want 2 (one per frame)", loader.calls)
	}
	if uploads.Load() != 2 || generations.Load() != 1 {
		t.Fatalf("uploads/generations = %d/%d, want 2/1", uploads.Load(), generations.Load())
	}
}
