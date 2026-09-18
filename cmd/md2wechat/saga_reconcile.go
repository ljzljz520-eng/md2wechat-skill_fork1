package main

import (
	"context"
	"fmt"
	"time"

	"github.com/geekjourneyx/md2wechat-skill/internal/saga"
	"github.com/geekjourneyx/md2wechat-skill/internal/wechat"
)

// sagaWechatBackend is the read-only WeChat query surface used during
// reconciliation. *wechat.Service satisfies it; tests use a fake.
type sagaWechatBackend interface {
	ListImageMaterialsSince(since, until time.Time, maxPages int) ([]wechat.MaterialCandidate, error)
	ListDraftsSince(since, until time.Time, maxPages int) ([]wechat.DraftCandidate, error)
}

// wechatSagaReconciler resolves unknown steps against the WeChat backend using
// time-window listing and content fingerprints.
type wechatSagaReconciler struct {
	ws sagaWechatBackend
}

func newWechatSagaReconciler(ws sagaWechatBackend, _ *saga.Operation) saga.Reconciler {
	if ws == nil {
		return nil
	}
	return &wechatSagaReconciler{ws: ws}
}

func (r *wechatSagaReconciler) Reconcile(_ context.Context, req saga.ReconcileRequest) (saga.Decision, error) {
	switch req.Kind {
	case saga.StepKindUploadMaterial:
		return r.reconcileMaterial(req)
	case saga.StepKindCreateDraft:
		return r.reconcileDraft(req)
	default:
		return saga.Decision{Outcome: saga.DecisionUnknown, Reason: fmt.Sprintf("unsupported step kind: %s", req.Kind)}, nil
	}
}

func (r *wechatSagaReconciler) reconcileMaterial(req saga.ReconcileRequest) (saga.Decision, error) {
	materials, err := r.ws.ListImageMaterialsSince(req.Window.Start, req.Window.End, 0)
	if err != nil {
		return saga.Decision{}, err
	}
	candidates := make([]wechat.MaterialCandidate, 0, len(materials))
	for _, item := range materials {
		if containsString(req.KnownRemoteIDs, item.MediaID) {
			continue
		}
		candidates = append(candidates, item)
	}
	switch len(candidates) {
	case 0:
		return saga.Decision{
			Outcome: saga.DecisionAbsent,
			Method:  "window_empty",
			Reason:  "attempt window contains no unclaimed image material; upload can be retried",
		}, nil
	case 1:
		item := candidates[0]
		effect := &saga.Effect{RemoteID: item.MediaID, RemoteURL: item.URL}
		return saga.Decision{
			Outcome:      saga.DecisionVerified,
			Method:       saga.VerifiedByWindowUnique,
			Effect:       effect,
			Compensation: &saga.Compensation{Kind: saga.CompensationDeleteMaterial, RemoteID: item.MediaID},
			Post: &saga.PostCondition{
				Kind:          postKindWechatImage,
				RemoteID:      item.MediaID,
				RemoteURL:     item.URL,
				ContentDigest: req.InputDigest,
				Extra:         map[string]any{"reconciled_at": time.Now().UTC().Format(time.RFC3339)},
			},
			Reason: "exactly one unclaimed image material exists in the attempt window",
		}, nil
	default:
		out := make([]saga.Candidate, 0, len(candidates))
		for _, item := range candidates {
			out = append(out, saga.Candidate{
				RemoteID:  item.MediaID,
				RemoteURL: item.URL,
				UpdatedAt: item.UpdateTime,
				Detail:    item.Name,
			})
		}
		return saga.Decision{
			Outcome:    saga.DecisionUnknown,
			Reason:     fmt.Sprintf("%d unclaimed image materials exist in the attempt window; manual review required", len(candidates)),
			Candidates: out,
		}, nil
	}
}

func (r *wechatSagaReconciler) reconcileDraft(req saga.ReconcileRequest) (saga.Decision, error) {
	drafts, err := r.ws.ListDraftsSince(req.Window.Start, req.Window.End, 0)
	if err != nil {
		return saga.Decision{}, err
	}

	var unclaimed []wechat.DraftCandidate
	var matches []wechat.DraftCandidate
	for _, item := range drafts {
		if containsString(req.KnownRemoteIDs, item.MediaID) {
			continue
		}
		unclaimed = append(unclaimed, item)
		if len(item.Articles) != 1 {
			continue
		}
		article := item.Articles[0]
		fingerprint := fingerprintDraftArticle(
			article.Title,
			article.Author,
			article.Digest,
			article.Content,
			article.ThumbMediaID,
			int(article.ShowCoverPic),
		)
		if fingerprint == req.InputDigest {
			matches = append(matches, item)
		}
	}

	switch {
	case len(matches) == 1:
		item := matches[0]
		article := item.Articles[0]
		return saga.Decision{
			Outcome: saga.DecisionVerified,
			Method:  saga.VerifiedByContentFingerprint,
			Effect:  &saga.Effect{RemoteID: item.MediaID},
			Compensation: &saga.Compensation{
				Kind:     saga.CompensationDeleteDraft,
				RemoteID: item.MediaID,
			},
			Post: &saga.PostCondition{
				Kind:          postKindWechatDraft,
				RemoteID:      item.MediaID,
				ContentDigest: req.InputDigest,
				Extra:         map[string]any{"title": article.Title},
			},
			Reason: "draft content fingerprint matches the recorded step input",
		}, nil
	case len(matches) > 1:
		return saga.Decision{
			Outcome:    saga.DecisionUnknown,
			Reason:     fmt.Sprintf("%d drafts match the content fingerprint; manual review required", len(matches)),
			Candidates: draftCandidates(matches),
		}, nil
	case len(unclaimed) == 0:
		return saga.Decision{
			Outcome: saga.DecisionAbsent,
			Method:  "window_empty",
			Reason:  "attempt window contains no unclaimed draft; draft creation can be retried",
		}, nil
	default:
		// Surface same-title drafts first since they are the likely duplicates.
		candidates := draftCandidates(unclaimed)
		return saga.Decision{
			Outcome:    saga.DecisionUnknown,
			Reason:     "drafts exist in the attempt window but none matches the content fingerprint; WeChat may have normalized the content",
			Candidates: candidates,
		}, nil
	}
}

func draftCandidates(items []wechat.DraftCandidate) []saga.Candidate {
	out := make([]saga.Candidate, 0, len(items))
	for _, item := range items {
		detail := ""
		if len(item.Articles) > 0 {
			detail = item.Articles[0].Title
		}
		out = append(out, saga.Candidate{
			RemoteID:  item.MediaID,
			UpdatedAt: item.UpdateTime,
			Detail:    detail,
		})
	}
	return out
}

func containsString(values []string, target string) bool {
	for _, value := range values {
		if value == target {
			return true
		}
	}
	return false
}
