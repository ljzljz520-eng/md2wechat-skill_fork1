package main

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/geekjourneyx/md2wechat-skill/internal/draft"
	"github.com/geekjourneyx/md2wechat-skill/internal/publish"
	"github.com/geekjourneyx/md2wechat-skill/internal/saga"
	"github.com/geekjourneyx/md2wechat-skill/internal/wechat"
)

const (
	sagaKindPublish     = "publish"
	sagaDirEnv          = "MD2WECHAT_SAGA_DIR"
	sagaDisableEnv      = "MD2WECHAT_SAGA"
	postKindWechatImage = "wechat_permanent_image"
	postKindWechatDraft = "wechat_draft"
	sagaRequestSchemaV1 = 1
)

// sagaPublishRequest is the sanitized operation descriptor persisted in the
// journal and used to reconstruct a `convert` invocation on resume. It never
// stores credentials.
type sagaPublishRequest struct {
	SchemaVersion  int    `json:"schema_version"`
	SourcePath     string `json:"source_path"`
	MarkdownDigest string `json:"markdown_digest"`
	Mode           string `json:"mode"`
	Theme          string `json:"theme"`
	FontSize       string `json:"font_size,omitempty"`
	BackgroundType string `json:"background_type,omitempty"`
	CustomPrompt   string `json:"custom_prompt,omitempty"`
	Title          string `json:"title,omitempty"`
	Author         string `json:"author,omitempty"`
	Digest         string `json:"digest,omitempty"`
	OutputFile     string `json:"output_file,omitempty"`
	SaveDraftPath  string `json:"save_draft_path,omitempty"`
	CoverImagePath string `json:"cover_image_path,omitempty"`
	CoverMediaID   string `json:"cover_media_id,omitempty"`
	Upload         bool   `json:"upload"`
	CreateDraft    bool   `json:"create_draft"`
	SaveDraft      bool   `json:"save_draft"`
	Account        string `json:"account,omitempty"`
	CLIVersion     string `json:"cli_version,omitempty"`
}

// sagaHarness wires the saga executor into the publish pipeline seam types.
type sagaHarness struct {
	store   *saga.Store
	exec    *saga.Executor
	request sagaPublishRequest
}

func sagaEnabled() bool {
	switch strings.ToLower(strings.TrimSpace(os.Getenv(sagaDisableEnv))) {
	case "off", "0", "false", "no":
		return false
	default:
		return true
	}
}

func sagaBaseDir() string {
	if dir := strings.TrimSpace(os.Getenv(sagaDirEnv)); dir != "" {
		return dir
	}
	homeDir, err := os.UserHomeDir()
	if err != nil {
		return filepath.Join(".", ".md2wechat-saga")
	}
	return filepath.Join(homeDir, ".config", "md2wechat", "saga")
}

func newSagaStore() *saga.Store {
	return saga.NewStore(sagaBaseDir())
}

func newOperationID() string {
	var raw [6]byte
	if _, err := rand.Read(raw[:]); err != nil {
		return "pub-" + fmt.Sprintf("%d", time.Now().UnixNano())
	}
	return "pub-" + hex.EncodeToString(raw[:])
}

// newSagaHarness begins or resumes the saga operation for a convert run.
func newSagaHarnessForConvert(input *publish.ConvertInput, explicitID string) (*sagaHarness, error) {
	descriptor := buildSagaPublishRequest(input)
	inputDigest, err := saga.DigestJSON(descriptor)
	if err != nil {
		return nil, err
	}
	store := newSagaStore()

	var journal *saga.Journal
	if strings.TrimSpace(explicitID) != "" {
		if existing, err := store.Open(explicitID); err == nil {
			if existing.InputDigest != inputDigest {
				return nil, fmt.Errorf("%w: operation %s belongs to a different input", saga.ErrInputDigestMismatch, explicitID)
			}
			journal, err = store.Resume(explicitID, saga.OpKindPublish, inputDigest, time.Now)
			if err != nil {
				return nil, err
			}
		} else if errors.Is(err, saga.ErrOperationNotFound) {
			journal, err = store.Begin(explicitID, saga.OpKindPublish, inputDigest, mustMarshalJSON(descriptor), time.Now)
			if err != nil {
				return nil, err
			}
		} else {
			return nil, err
		}
	} else if existing, err := store.FindByInputDigest(saga.OpKindPublish, inputDigest); err == nil {
		journal, err = store.Resume(existing.ID, saga.OpKindPublish, inputDigest, time.Now)
		if err != nil {
			return nil, err
		}
	} else {
		journal, err = store.Begin(newOperationID(), saga.OpKindPublish, inputDigest, mustMarshalJSON(descriptor), time.Now)
		if err != nil {
			return nil, err
		}
	}

	reconciler := newWechatSagaReconciler(wechat.NewService(cfg, log), journal.Op())
	return &sagaHarness{
		store:   store,
		exec:    saga.NewExecutor(store, journal, reconciler),
		request: descriptor,
	}, nil
}

func buildSagaPublishRequest(input *publish.ConvertInput) sagaPublishRequest {
	req := input.ConvertRequest
	sourcePath := input.Source.Path
	if abs, err := filepath.Abs(sourcePath); err == nil {
		sourcePath = abs
	}
	return sagaPublishRequest{
		SchemaVersion:  sagaRequestSchemaV1,
		SourcePath:     sourcePath,
		MarkdownDigest: saga.DigestBytes([]byte(req.Markdown)),
		Mode:           string(req.Mode),
		Theme:          req.Theme,
		FontSize:       req.FontSize,
		BackgroundType: req.BackgroundType,
		CustomPrompt:   req.CustomPrompt,
		Title:          input.Source.Metadata.Title,
		Author:         input.Source.Metadata.Author,
		Digest:         input.Source.Metadata.Digest,
		OutputFile:     input.OutputFile,
		SaveDraftPath:  input.SaveDraftPath,
		CoverImagePath: input.CoverImagePath,
		CoverMediaID:   input.CoverMediaID,
		Upload:         input.Intent.Upload,
		CreateDraft:    input.Intent.CreateDraft,
		SaveDraft:      input.Intent.SaveDraft,
		Account:        wechatAccountName,
		CLIVersion:     Version,
	}
}

func mustMarshalJSON(v any) json.RawMessage {
	data, err := json.Marshal(v)
	if err != nil {
		panic(err)
	}
	return data
}

// wrapService installs the idempotency hooks into a publish service.
func (h *sagaHarness) wrapService(svc *publish.Service) {
	svc.SetAssetStepHook(h)
	svc.SetDraftCreator(h.draftCreator(svc.DraftCreator()))
	svc.SetCoverUploader(h.coverUploader(svc.CoverUploader()))
}

// Close releases the journal lock.
func (h *sagaHarness) Close() {
	if h != nil && h.exec != nil {
		_ = h.exec.Journal().Close()
	}
}

// Report returns the current saga report.
func (h *sagaHarness) Report() *saga.Report {
	if h == nil || h.exec == nil {
		return nil
	}
	return h.exec.Report()
}

// Finish records the terminal status.
func (h *sagaHarness) Finish() (saga.OpStatus, error) {
	return h.exec.Finish()
}

// RunAssetStep implements publish.AssetStepHook.
func (h *sagaHarness) RunAssetStep(asset publish.AssetRef, work func() (string, string, error)) (string, string, error) {
	inputDigest, err := assetStepDigest(asset)
	if err != nil {
		return "", "", err
	}
	stepID := fmt.Sprintf("asset:%d", asset.Index)
	effect, err := h.exec.RunStep(
		context.Background(),
		stepID,
		saga.StepKindUploadMaterial,
		inputDigest,
		saga.CompensationDeleteMaterial,
		func(e *saga.Effect) saga.PostCondition {
			return saga.PostCondition{
				Kind:          postKindWechatImage,
				RemoteID:      e.RemoteID,
				RemoteURL:     e.RemoteURL,
				ContentDigest: inputDigest,
				Extra: map[string]any{
					"index":  asset.Index,
					"kind":   string(asset.Kind),
					"source": asset.Source,
				},
			}
		},
		func() (*saga.Effect, error) {
			mediaID, publicURL, err := work()
			if err != nil {
				return nil, err
			}
			return &saga.Effect{RemoteID: mediaID, RemoteURL: publicURL}, nil
		},
	)
	if err != nil {
		return "", "", err
	}
	return effect.RemoteID, effect.RemoteURL, nil
}

func assetStepDigest(asset publish.AssetRef) (string, error) {
	switch asset.Kind {
	case publish.AssetKindLocal:
		path := asset.ResolvedSource
		if path == "" {
			path = asset.Source
		}
		fileDigest, err := saga.DigestFile(path)
		if err != nil {
			return "", fmt.Errorf("digest local asset: %w", err)
		}
		return saga.DigestStrings("local", path, fileDigest), nil
	case publish.AssetKindRemote:
		return saga.DigestStrings("remote", asset.Source), nil
	case publish.AssetKindAI:
		return saga.DigestStrings("ai", asset.Prompt), nil
	default:
		return "", fmt.Errorf("unsupported asset kind: %s", asset.Kind)
	}
}

// coverUploader wraps the cover upload side effect as step "cover".
func (h *sagaHarness) coverUploader(inner publish.CoverUploader) publish.CoverUploader {
	if inner == nil {
		return nil
	}
	return func(imagePath string) (string, error) {
		fileDigest, err := saga.DigestFile(imagePath)
		if err != nil {
			return "", fmt.Errorf("digest cover image: %w", err)
		}
		inputDigest := saga.DigestStrings("cover", imagePath, fileDigest)
		effect, err := h.exec.RunStep(
			context.Background(),
			"cover",
			saga.StepKindUploadMaterial,
			inputDigest,
			saga.CompensationDeleteMaterial,
			func(e *saga.Effect) saga.PostCondition {
				return saga.PostCondition{
					Kind:          postKindWechatImage,
					RemoteID:      e.RemoteID,
					RemoteURL:     e.RemoteURL,
					ContentDigest: inputDigest,
					Extra:         map[string]any{"role": "cover", "source": imagePath},
				}
			},
			func() (*saga.Effect, error) {
				mediaID, err := inner(imagePath)
				if err != nil {
					return nil, err
				}
				return &saga.Effect{RemoteID: mediaID}, nil
			},
		)
		if err != nil {
			return "", err
		}
		return effect.RemoteID, nil
	}
}

// sagaDraftCreator wraps draft creation as step "draft".
type sagaDraftCreator struct {
	harness *sagaHarness
	inner   publish.DraftCreator
}

func (h *sagaHarness) draftCreator(inner publish.DraftCreator) publish.DraftCreator {
	if inner == nil {
		return nil
	}
	return &sagaDraftCreator{harness: h, inner: inner}
}

func (c *sagaDraftCreator) CreateDraft(artifact publish.Artifact) (*publish.DraftResult, error) {
	inputDigest := artifactDraftFingerprint(artifact)
	effect, err := c.harness.exec.RunStep(
		context.Background(),
		"draft",
		saga.StepKindCreateDraft,
		inputDigest,
		saga.CompensationDeleteDraft,
		func(e *saga.Effect) saga.PostCondition {
			return saga.PostCondition{
				Kind:          postKindWechatDraft,
				RemoteID:      e.RemoteID,
				RemoteURL:     e.RemoteURL,
				ContentDigest: inputDigest,
				Extra:         map[string]any{"title": artifact.Metadata.Title},
			}
		},
		func() (*saga.Effect, error) {
			result, err := c.inner.CreateDraft(artifact)
			if err != nil {
				return nil, err
			}
			return &saga.Effect{RemoteID: result.MediaID, RemoteURL: result.DraftURL}, nil
		},
	)
	if err != nil {
		return nil, err
	}
	return &publish.DraftResult{MediaID: effect.RemoteID, DraftURL: effect.RemoteURL}, nil
}

// draftFingerprintPayload is the exact article shape sent to WeChat. Field
// order is part of the fingerprint contract and must remain stable.
type draftFingerprintPayload struct {
	Title        string `json:"title"`
	Author       string `json:"author"`
	Digest       string `json:"digest"`
	Content      string `json:"content"`
	ThumbMediaID string `json:"thumb_media_id"`
	ShowCoverPic int    `json:"show_cover_pic"`
}

// artifactDraftFingerprint mirrors internal/draft.ArtifactDraftCreator mapping,
// including the generated-digest fallback applied before upload.
func artifactDraftFingerprint(artifact publish.Artifact) string {
	digestText := artifact.Metadata.Digest
	if digestText == "" {
		digestText = draft.GenerateDigestFromContent(artifact.HTML, 120)
	}
	showCoverPic := 0
	if artifact.CoverMediaID != "" {
		showCoverPic = 1
	}
	return fingerprintDraftArticle(artifact.Metadata.Title, artifact.Metadata.Author, digestText, artifact.HTML, artifact.CoverMediaID, showCoverPic)
}

func fingerprintDraftArticle(title, author, digestText, content, thumbMediaID string, showCoverPic int) string {
	return saga.MustDigestJSON(draftFingerprintPayload{
		Title:        title,
		Author:       author,
		Digest:       digestText,
		Content:      content,
		ThumbMediaID: thumbMediaID,
		ShowCoverPic: showCoverPic,
	})
}
