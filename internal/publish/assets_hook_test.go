package publish

import (
	"errors"
	"strings"
	"testing"

	"github.com/geekjourneyx/md2wechat-skill/internal/image"
)

// recordingHook simulates saga semantics across processes: the first
// invocation executes work, later invocations replay the recorded effect
// without touching the remote backend.
type recordingHook struct {
	workCalls int
	effects   map[string]recordedEffect
	failFor   string
}

type recordedEffect struct {
	mediaID   string
	publicURL string
}

func (h *recordingHook) RunAssetStep(asset AssetRef, work func() (string, string, error)) (string, string, error) {
	key := asset.Source
	if h.effects == nil {
		h.effects = map[string]recordedEffect{}
	}
	if effect, ok := h.effects[key]; ok {
		return effect.mediaID, effect.publicURL, nil
	}
	if h.failFor != "" && key == h.failFor {
		return "", "", errors.New("hook blocked")
	}
	h.workCalls++
	mediaID, publicURL, err := work()
	if err != nil {
		return "", "", err
	}
	h.effects[key] = recordedEffect{mediaID: mediaID, publicURL: publicURL}
	return mediaID, publicURL, nil
}

func processInputForHook(html string, assets []AssetRef) *ProcessInput {
	return &ProcessInput{HTML: html, Assets: assets}
}

func TestAssetPipelineHookReplaysEffectWithoutWork(t *testing.T) {
	processor := &fakeAssetProcessor{
		onlineResults: map[string]*image.UploadResult{
			"https://example.com/a.png": {MediaID: "m-a", WechatURL: "https://wechat.local/a"},
		},
	}
	hook := &recordingHook{}
	pipeline := NewAssetPipeline(processor).WithAssetStepHook(hook)
	input := processInputForHook(
		`<img src="https://example.com/a.png">`,
		[]AssetRef{{Index: 0, Kind: AssetKindRemote, Source: "https://example.com/a.png", Placeholder: "<!-- IMG:0 -->"}},
	)

	for i := 0; i < 2; i++ {
		output, err := pipeline.Process(input)
		if err != nil {
			t.Fatalf("Process %d: %v", i, err)
		}
		if !strings.Contains(output.HTML, "https://wechat.local/a") {
			t.Fatalf("Process %d HTML = %s", i, output.HTML)
		}
	}
	if hook.workCalls != 1 {
		t.Fatalf("hook work calls = %d, want 1", hook.workCalls)
	}
	if len(processor.onlineCalls) != 1 {
		t.Fatalf("processor calls = %d, want 1 (replay must not re-upload)", len(processor.onlineCalls))
	}
}

func TestAssetPipelineWithoutHookCallsProcessorEveryTime(t *testing.T) {
	processor := &fakeAssetProcessor{
		onlineResults: map[string]*image.UploadResult{
			"https://example.com/a.png": {MediaID: "m-a", WechatURL: "https://wechat.local/a"},
		},
	}
	pipeline := NewAssetPipeline(processor)
	input := processInputForHook(
		`<img src="https://example.com/a.png">`,
		[]AssetRef{{Index: 0, Kind: AssetKindRemote, Source: "https://example.com/a.png", Placeholder: "<!-- IMG:0 -->"}},
	)
	for i := 0; i < 2; i++ {
		if _, err := pipeline.Process(input); err != nil {
			t.Fatalf("Process %d: %v", i, err)
		}
	}
	if len(processor.onlineCalls) != 2 {
		t.Fatalf("processor calls = %d, want 2 without hook", len(processor.onlineCalls))
	}
}

func TestAssetPipelineHookFailureFailsPipeline(t *testing.T) {
	processor := &fakeAssetProcessor{
		onlineResults: map[string]*image.UploadResult{
			"https://example.com/a.png": {MediaID: "m-a", WechatURL: "https://wechat.local/a"},
		},
	}
	hook := &recordingHook{failFor: "https://example.com/a.png"}
	pipeline := NewAssetPipeline(processor).WithAssetStepHook(hook)
	_, err := pipeline.Process(processInputForHook(
		`<img src="https://example.com/a.png">`,
		[]AssetRef{{Index: 0, Kind: AssetKindRemote, Source: "https://example.com/a.png", Placeholder: "<!-- IMG:0 -->"}},
	))
	if err == nil || !strings.Contains(err.Error(), "hook blocked") {
		t.Fatalf("err = %v, want hook blocked", err)
	}
	if len(processor.onlineCalls) != 0 {
		t.Fatalf("processor must not be called when hook rejects, calls = %d", len(processor.onlineCalls))
	}
}
