package main

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/geekjourneyx/md2wechat-skill/internal/saga"
	"github.com/geekjourneyx/md2wechat-skill/internal/wechat"
)

type fakeSagaBackend struct {
	materials  []wechat.MaterialCandidate
	drafts     []wechat.DraftCandidate
	err        error
	matCalls   int
	draftCalls int
}

func (f *fakeSagaBackend) ListImageMaterialsSince(_, _ time.Time, _ int) ([]wechat.MaterialCandidate, error) {
	f.matCalls++
	return f.materials, f.err
}

func (f *fakeSagaBackend) ListDraftsSince(_, _ time.Time, _ int) ([]wechat.DraftCandidate, error) {
	f.draftCalls++
	return f.drafts, f.err
}

func reconcileWindow() saga.TimeWindow {
	now := time.Now()
	return saga.TimeWindow{Start: now.Add(-time.Minute), End: now.Add(time.Minute)}
}

func TestReconcileMaterialWindowMatrix(t *testing.T) {
	tests := []struct {
		name      string
		materials []wechat.MaterialCandidate
		siblings  []string
		want      saga.DecisionOutcome
		wantID    string
		wantCandN int
	}{
		{
			name:      "empty window means absent and retry is safe",
			materials: nil,
			want:      saga.DecisionAbsent,
		},
		{
			name: "single unclaimed material is adopted by window uniqueness",
			materials: []wechat.MaterialCandidate{
				{MediaID: "m-1", URL: "https://mm.example/m-1", Name: "x.png", UpdateTime: time.Now()},
			},
			want:   saga.DecisionVerified,
			wantID: "m-1",
		},
		{
			name: "sibling effects are excluded before matching",
			materials: []wechat.MaterialCandidate{
				{MediaID: "sibling", URL: "https://mm.example/sibling"},
			},
			siblings: []string{"sibling"},
			want:     saga.DecisionAbsent,
		},
		{
			name: "multiple unclaimed materials stay unknown",
			materials: []wechat.MaterialCandidate{
				{MediaID: "m-1", URL: "https://mm.example/1"},
				{MediaID: "m-2", URL: "https://mm.example/2"},
			},
			want:      saga.DecisionUnknown,
			wantCandN: 2,
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			backend := &fakeSagaBackend{materials: tc.materials}
			recon := newWechatSagaReconciler(backend, nil)
			decision, err := recon.Reconcile(context.Background(), saga.ReconcileRequest{
				Kind:           saga.StepKindUploadMaterial,
				InputDigest:    "input-digest",
				Window:         reconcileWindow(),
				KnownRemoteIDs: tc.siblings,
			})
			if err != nil {
				t.Fatal(err)
			}
			if decision.Outcome != tc.want {
				t.Fatalf("outcome = %s, want %s (%s)", decision.Outcome, tc.want, decision.Reason)
			}
			if tc.wantID != "" {
				if decision.Effect == nil || decision.Effect.RemoteID != tc.wantID {
					t.Fatalf("effect = %#v", decision.Effect)
				}
				if decision.Compensation == nil || decision.Compensation.Kind != saga.CompensationDeleteMaterial {
					t.Fatalf("compensation = %#v", decision.Compensation)
				}
				if decision.Post == nil || decision.Post.ContentDigest != "input-digest" {
					t.Fatalf("post = %#v", decision.Post)
				}
			}
			if len(decision.Candidates) != tc.wantCandN {
				t.Fatalf("candidates = %#v", decision.Candidates)
			}
		})
	}
}

func TestReconcileMaterialBackendErrorPropagated(t *testing.T) {
	backend := &fakeSagaBackend{err: errors.New("500 internal")}
	recon := newWechatSagaReconciler(backend, nil)
	if _, err := recon.Reconcile(context.Background(), saga.ReconcileRequest{
		Kind:   saga.StepKindUploadMaterial,
		Window: reconcileWindow(),
	}); err == nil {
		t.Fatal("backend error swallowed")
	}
}

func TestReconcileDraftFingerprintMatrix(t *testing.T) {
	matched := fingerprintDraftArticle("标题", "作者", "摘要", "<p>html</p>", "cover-1", 1)

	tests := []struct {
		name      string
		drafts    []wechat.DraftCandidate
		siblings  []string
		want      saga.DecisionOutcome
		wantID    string
		wantCands int
	}{
		{
			name:   "empty draft window means absent",
			drafts: nil,
			want:   saga.DecisionAbsent,
		},
		{
			name: "content fingerprint match is verified",
			drafts: []wechat.DraftCandidate{{
				MediaID: "draft-match",
				Articles: []wechat.DraftArticleSnapshot{{
					Title: "标题", Author: "作者", Digest: "摘要", Content: "<p>html</p>",
					ThumbMediaID: "cover-1", ShowCoverPic: 1,
				}},
			}},
			want:   saga.DecisionVerified,
			wantID: "draft-match",
		},
		{
			name: "fingerprint mismatch stays unknown with candidates",
			drafts: []wechat.DraftCandidate{{
				MediaID:  "draft-other",
				Articles: []wechat.DraftArticleSnapshot{{Title: "其他", Content: "<p>other</p>", ThumbMediaID: "cover-2"}},
			}},
			want:      saga.DecisionUnknown,
			wantCands: 1,
		},
		{
			name: "multiple fingerprint matches stay unknown",
			drafts: []wechat.DraftCandidate{
				{MediaID: "d1", Articles: []wechat.DraftArticleSnapshot{{
					Title: "标题", Author: "作者", Digest: "摘要", Content: "<p>html</p>", ThumbMediaID: "cover-1", ShowCoverPic: 1,
				}}},
				{MediaID: "d2", Articles: []wechat.DraftArticleSnapshot{{
					Title: "标题", Author: "作者", Digest: "摘要", Content: "<p>html</p>", ThumbMediaID: "cover-1", ShowCoverPic: 1,
				}}},
			},
			want:      saga.DecisionUnknown,
			wantCands: 2,
		},
		{
			name: "sibling drafts are excluded before fingerprinting",
			drafts: []wechat.DraftCandidate{{
				MediaID: "sibling-draft",
				Articles: []wechat.DraftArticleSnapshot{{
					Title: "标题", Author: "作者", Digest: "摘要", Content: "<p>html</p>", ThumbMediaID: "cover-1", ShowCoverPic: 1,
				}},
			}},
			siblings: []string{"sibling-draft"},
			want:     saga.DecisionAbsent,
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			backend := &fakeSagaBackend{drafts: tc.drafts}
			recon := newWechatSagaReconciler(backend, nil)
			decision, err := recon.Reconcile(context.Background(), saga.ReconcileRequest{
				Kind:           saga.StepKindCreateDraft,
				InputDigest:    matched,
				Window:         reconcileWindow(),
				KnownRemoteIDs: tc.siblings,
			})
			if err != nil {
				t.Fatal(err)
			}
			if decision.Outcome != tc.want {
				t.Fatalf("outcome = %s, want %s (%s)", decision.Outcome, tc.want, decision.Reason)
			}
			if tc.wantID != "" {
				if decision.Effect == nil || decision.Effect.RemoteID != tc.wantID {
					t.Fatalf("effect = %#v", decision.Effect)
				}
				if decision.Method != saga.VerifiedByContentFingerprint {
					t.Fatalf("method = %s", decision.Method)
				}
			}
			if len(decision.Candidates) != tc.wantCands {
				t.Fatalf("candidates = %#v", decision.Candidates)
			}
		})
	}
}
