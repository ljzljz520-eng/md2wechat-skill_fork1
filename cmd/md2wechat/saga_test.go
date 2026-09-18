package main

import (
	"encoding/json"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/geekjourneyx/md2wechat-skill/internal/config"
	"github.com/geekjourneyx/md2wechat-skill/internal/converter"
	"github.com/geekjourneyx/md2wechat-skill/internal/image"
	"github.com/geekjourneyx/md2wechat-skill/internal/publish"
	"github.com/geekjourneyx/md2wechat-skill/internal/saga"
	"go.uber.org/zap"
)

func setupSagaCLITest(t *testing.T) {
	t.Helper()
	oldCfg, oldLog, oldJSON := cfg, log, jsonOutput
	oldAccount := wechatAccountName
	t.Cleanup(func() {
		cfg, log, jsonOutput = oldCfg, oldLog, oldJSON
		wechatAccountName = oldAccount
	})
	cfg = &config.Config{DefaultTheme: "default"}
	log = zap.NewNop()
	jsonOutput = true
}

func seedSagaOp(t *testing.T, store *saga.Store, id string, at time.Time, terminal *saga.OpStatus, unknown bool) {
	t.Helper()
	clock := func() time.Time { return at }
	j, err := store.Begin(id, saga.OpKindPublish, "digest-"+id, json.RawMessage(`{}`), clock)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := j.PlanStep("asset:0", saga.StepKindUploadMaterial, "step-digest"); err != nil {
		t.Fatal(err)
	}
	attempt, err := j.AttemptStarted("asset:0")
	if err != nil {
		t.Fatal(err)
	}
	if unknown {
		if err := j.AttemptFailed("asset:0", attempt, os.ErrDeadlineExceeded, true); err != nil {
			t.Fatal(err)
		}
	} else {
		if err := j.StepEffect("asset:0", attempt,
			saga.Effect{RemoteID: "media-1"},
			saga.Compensation{Kind: saga.CompensationDeleteMaterial, RemoteID: "media-1"},
			saga.PostCondition{Kind: "wechat_permanent_image", RemoteID: "media-1"},
		); err != nil {
			t.Fatal(err)
		}
	}
	if terminal != nil {
		if err := j.Terminal(*terminal); err != nil {
			t.Fatal(err)
		}
	}
	if err := j.Close(); err != nil {
		t.Fatal(err)
	}
}

func TestSagaListAndStatusEnvelope(t *testing.T) {
	setupSagaCLITest(t)
	dir := t.TempDir()
	t.Setenv(sagaDirEnv, dir)
	store := saga.NewStore(dir)

	completed := saga.OpStatusCompleted
	seedSagaOp(t, store, "pub-done", time.Date(2025, 1, 2, 3, 4, 5, 0, time.UTC), &completed, false)
	seedSagaOp(t, store, "pub-unknown", time.Date(2025, 1, 2, 4, 5, 6, 0, time.UTC), nil, true)

	stdout := captureStdout(t, func() {
		sagaCmd.SetArgs([]string{"list"})
		if err := sagaCmd.Execute(); err != nil {
			t.Fatalf("saga list: %v", err)
		}
	})
	resp := decodeResponse(t, stdout)
	if resp["code"] != "SAGA_LISTED" {
		t.Fatalf("code = %v", resp["code"])
	}
	items, ok := resp["data"].([]any)
	if !ok || len(items) != 2 {
		t.Fatalf("data = %#v", resp["data"])
	}

	stdout = captureStdout(t, func() {
		sagaCmd.SetArgs([]string{"status", "pub-unknown"})
		if err := sagaCmd.Execute(); err != nil {
			t.Fatalf("saga status: %v", err)
		}
	})
	resp = decodeResponse(t, stdout)
	if resp["code"] != codeSagaManualActionRequired || resp["status"] != "action_required" {
		t.Fatalf("unknown status response = %s", stdout)
	}
	data := responseData(t, resp)
	if data["status"] != string(saga.OpStatusUnknown) {
		t.Fatalf("data status = %v", data["status"])
	}
	actions, ok := data["manual_actions"].([]any)
	if !ok || len(actions) != 1 {
		t.Fatalf("manual_actions = %#v", data["manual_actions"])
	}

	stdout = captureStdout(t, func() {
		sagaCmd.SetArgs([]string{"status", "pub-done"})
		if err := sagaCmd.Execute(); err != nil {
			t.Fatalf("saga status done: %v", err)
		}
	})
	resp = decodeResponse(t, stdout)
	if resp["code"] != "SAGA_COMPLETED" {
		t.Fatalf("completed status response = %s", stdout)
	}

	// No argument selects the newest operation.
	stdout = captureStdout(t, func() {
		sagaCmd.SetArgs([]string{"status"})
		if err := sagaCmd.Execute(); err != nil {
			t.Fatalf("saga status latest: %v", err)
		}
	})
	if id := responseData(t, decodeResponse(t, stdout))["operation_id"]; id != "pub-unknown" {
		t.Fatalf("latest operation = %v", id)
	}

	// Missing operation is a CLI error, not an empty envelope.
	err := runSagaStatus([]string{"pub-missing"})
	cliErr, ok := extractCLIError(err)
	if !ok || cliErr.Code != codeConvertInvalid {
		t.Fatalf("missing status err = %v", err)
	}
}

func TestSagaResumeRejectsChangedSource(t *testing.T) {
	setupSagaCLITest(t)
	dir := t.TempDir()
	t.Setenv(sagaDirEnv, dir)

	article := filepath.Join(dir, "article.md")
	if err := os.WriteFile(article, []byte("# Title\n\nbody"), 0o600); err != nil {
		t.Fatal(err)
	}
	document := converter.ParseArticleDocument("# Title\n\nbody")
	descriptor := sagaPublishRequest{
		SchemaVersion:  sagaRequestSchemaV1,
		SourcePath:     article,
		MarkdownDigest: saga.DigestBytes([]byte(document.Body)),
		Mode:           "api",
		Theme:          "default",
		Upload:         true,
	}
	raw, _ := json.Marshal(descriptor)
	store := saga.NewStore(dir)
	j, err := store.Begin("pub-changed", saga.OpKindPublish, "op-digest", raw, time.Now)
	if err != nil {
		t.Fatal(err)
	}
	if err := j.Close(); err != nil {
		t.Fatal(err)
	}

	if err := os.WriteFile(article, []byte("# Title\n\nchanged body"), 0o600); err != nil {
		t.Fatal(err)
	}
	err = runSagaResume("pub-changed")
	cliErr, ok := extractCLIError(err)
	if !ok || cliErr.Code != codeConvertInvalid {
		t.Fatalf("resume changed source err = %v", err)
	}
}

// TestSagaConvertIsIdempotentAcrossRunsAndResume proves the core duplicate
// protection contract end to end: re-running the same convert (and
// `saga resume`) never uploads or creates a draft a second time.
func TestSagaConvertIsIdempotentAcrossRunsAndResume(t *testing.T) {
	setupSagaCLITest(t)
	sagaDir := t.TempDir()
	t.Setenv(sagaDirEnv, sagaDir)
	t.Setenv(sagaDisableEnv, "on")

	oldCfg, oldLog := cfg, log
	oldMode, oldTheme, oldAPIKey := convertMode, convertTheme, convertAPIKey
	oldFontSize, oldBackground := convertFontSize, convertBackgroundType
	oldCustomPrompt, oldOutput := convertCustomPrompt, convertOutput
	oldPreview, oldUpload, oldDraft := convertPreview, convertUpload, convertDraft
	oldSaveDraft, oldCover, oldCoverMediaID := convertSaveDraft, convertCoverImage, convertCoverMediaID
	oldTitle, oldAuthor, oldDigest := convertTitle, convertAuthor, convertDigest
	oldOperationID := convertOperationID
	oldNewConverter, oldNewProcessor := newMarkdownConverter, newImageProcessor
	oldNewDraftCreator, oldUploadCover := newDraftCreator, uploadCoverImageFn
	t.Cleanup(func() {
		cfg, log = oldCfg, oldLog
		convertMode, convertTheme, convertAPIKey = oldMode, oldTheme, oldAPIKey
		convertFontSize, convertBackgroundType = oldFontSize, oldBackground
		convertCustomPrompt, convertOutput = oldCustomPrompt, oldOutput
		convertPreview, convertUpload, convertDraft = oldPreview, oldUpload, oldDraft
		convertSaveDraft, convertCoverImage, convertCoverMediaID = oldSaveDraft, oldCover, oldCoverMediaID
		convertTitle, convertAuthor, convertDigest = oldTitle, oldAuthor, oldDigest
		convertOperationID = oldOperationID
		newMarkdownConverter, newImageProcessor = oldNewConverter, oldNewProcessor
		newDraftCreator, uploadCoverImageFn = oldNewDraftCreator, oldUploadCover
	})

	cfg = &config.Config{
		WechatAppID:        "appid",
		WechatSecret:       "secret",
		MD2WechatAPIKey:    "api-key",
		DefaultConvertMode: "api",
		MaxImageWidth:      1920,
		MaxImageSize:       5 * 1024 * 1024,
		HTTPTimeout:        30,
		DefaultTheme:       "default",
	}
	log = zap.NewNop()
	convertMode = "api"
	convertTheme = "default"
	convertAPIKey = ""
	convertFontSize = "medium"
	convertBackgroundType = "default"
	convertCustomPrompt = ""
	convertOutput = ""
	convertPreview = false
	convertUpload = false
	convertDraft = true
	convertSaveDraft = ""
	convertCoverImage = ""
	convertCoverMediaID = "existing-cover"
	convertTitle, convertAuthor, convertDigest = "", "", ""
	convertOperationID = ""

	article := filepath.Join(sagaDir, "article.md")
	if err := os.WriteFile(article, []byte("# 标题\n\n![](https://example.com/a.png)\n"), 0o600); err != nil {
		t.Fatal(err)
	}

	conv := &fakeConverter{
		result: &converter.ConvertResult{
			Success: true,
			Mode:    converter.ModeAPI,
			Theme:   "default",
			HTML:    `<p>x</p><img src="https://cdn.example.com/1">`,
			Images: []converter.ImageRef{
				{Index: 0, Type: converter.ImageTypeOnline, Original: "https://example.com/a.png", Placeholder: "<!-- IMG:0 -->"},
			},
		},
	}
	processor := &fakeImageProcessor{
		onlineResults: map[string]*image.UploadResult{
			"https://example.com/a.png": {MediaID: "m-a", WechatURL: "https://wechat.local/a"},
		},
	}
	drafter := &fakeDraftCreator{result: &publish.DraftResult{MediaID: "draft-1"}}
	newMarkdownConverter = func() converter.Converter { return conv }
	newImageProcessor = func() imageProcessor { return processor }
	newDraftCreator = func() publish.DraftCreator { return drafter }
	uploadCoverImageFn = func(string) (string, error) {
		t.Fatal("cover uploader must not be called with --cover-media-id")
		return "", nil
	}

	runOne := func(label string) map[string]any {
		var resp map[string]any
		stdout := captureStdout(t, func() {
			if err := runConvert(nil, []string{article}); err != nil {
				t.Fatalf("%s: %v", label, err)
			}
		})
		resp = decodeResponse(t, stdout)
		if resp["success"] != true {
			t.Fatalf("%s response = %s", label, stdout)
		}
		return responseData(t, resp)
	}

	data1 := runOne("first convert")
	saga1, _ := data1["saga"].(map[string]any)
	if saga1 == nil || saga1["status"] != string(saga.OpStatusCompleted) {
		t.Fatalf("first run saga = %#v", data1["saga"])
	}
	opID, _ := saga1["operation_id"].(string)
	if opID == "" {
		t.Fatal("missing operation id")
	}

	data2 := runOne("second convert")
	saga2, _ := data2["saga"].(map[string]any)
	if saga2["operation_id"] != opID || saga2["status"] != string(saga.OpStatusCompleted) {
		t.Fatalf("second run must resume the same operation: %#v", saga2)
	}

	// `saga resume <id>` rebuilds the invocation from the journal descriptor.
	var resumeResp map[string]any
	stdout := captureStdout(t, func() {
		sagaCmd.SetArgs([]string{"resume", opID})
		if err := sagaCmd.Execute(); err != nil {
			t.Fatalf("saga resume: %v", err)
		}
	})
	resumeResp = decodeResponse(t, stdout)
	if resumeResp["success"] != true {
		t.Fatalf("resume response = %s", stdout)
	}

	if len(processor.onlineCalls) != 1 {
		t.Fatalf("online uploads = %d, want exactly 1 across 3 runs", len(processor.onlineCalls))
	}
	if len(drafter.artifacts) != 1 {
		t.Fatalf("draft creations = %d, want exactly 1 across 3 runs", len(drafter.artifacts))
	}
}
