package wechat

import (
	"fmt"
	"time"

	"github.com/silenceper/wechat/v2/officialaccount/draft"
	"github.com/silenceper/wechat/v2/officialaccount/material"
)

// Reconcile page size and safety bound for backend listing scans.
const (
	reconcilePageSize = 20
	reconcileMaxPages = 5
)

// MaterialCandidate is one permanent image material observed remotely.
type MaterialCandidate struct {
	MediaID    string    `json:"media_id"`
	URL        string    `json:"url"`
	Name       string    `json:"name,omitempty"`
	UpdateTime time.Time `json:"update_time"`
}

// DraftArticleSnapshot is the content-bearing subset of a draft article.
type DraftArticleSnapshot struct {
	Title        string `json:"title"`
	Author       string `json:"author,omitempty"`
	Digest       string `json:"digest,omitempty"`
	Content      string `json:"content"`
	ThumbMediaID string `json:"thumb_media_id,omitempty"`
	ShowCoverPic uint   `json:"show_cover_pic,omitempty"`
}

// DraftCandidate is one draft observed in the draft box.
type DraftCandidate struct {
	MediaID    string                 `json:"media_id"`
	UpdateTime time.Time              `json:"update_time"`
	Articles   []DraftArticleSnapshot `json:"articles"`
}

// ListImageMaterialsSince returns permanent image materials whose update_time
// falls within [since, until]. Results are ordered newest first and scanning is
// bounded by maxPages (0 uses the default safety bound).
func (s *Service) ListImageMaterialsSince(since, until time.Time, maxPages int) ([]MaterialCandidate, error) {
	if maxPages <= 0 {
		maxPages = reconcileMaxPages
	}
	var out []MaterialCandidate
	err := s.withWechatSDKHTTPClient(func() error {
		mat := s.getOfficialAccount().GetMaterial()
		for page := 0; page < maxPages; page++ {
			offset := int64(page) * reconcilePageSize
			list, err := mat.BatchGetMaterial(material.PermanentMaterialTypeImage, offset, reconcilePageSize)
			if err != nil {
				return fmt.Errorf("list image materials: %w", err)
			}
			if len(list.Item) == 0 {
				return nil
			}
			reachedBeforeWindow := false
			for _, item := range list.Item {
				updated := time.Unix(item.UpdateTime, 0)
				if !since.IsZero() && updated.Before(since) {
					reachedBeforeWindow = true
					continue
				}
				if !until.IsZero() && updated.After(until) {
					continue
				}
				out = append(out, MaterialCandidate{
					MediaID:    item.MediaID,
					URL:        item.URL,
					Name:       item.Name,
					UpdateTime: updated,
				})
			}
			if reachedBeforeWindow || int64(len(list.Item)) < reconcilePageSize {
				return nil
			}
		}
		return nil
	})
	if err != nil {
		return nil, err
	}
	return out, nil
}

// ListDraftsSince returns drafts whose update_time falls within [since, until],
// including article content for fingerprint comparison.
func (s *Service) ListDraftsSince(since, until time.Time, maxPages int) ([]DraftCandidate, error) {
	if maxPages <= 0 {
		maxPages = reconcileMaxPages
	}
	var out []DraftCandidate
	err := s.withWechatSDKHTTPClient(func() error {
		dm := s.getOfficialAccount().GetDraft()
		for page := 0; page < maxPages; page++ {
			offset := int64(page) * reconcilePageSize
			list, err := dm.PaginateDraft(offset, reconcilePageSize, false)
			if err != nil {
				return fmt.Errorf("list drafts: %w", err)
			}
			if len(list.Item) == 0 {
				return nil
			}
			reachedBeforeWindow := false
			for _, item := range list.Item {
				updated := time.Unix(item.UpdateTime, 0)
				if !since.IsZero() && updated.Before(since) {
					reachedBeforeWindow = true
					continue
				}
				if !until.IsZero() && updated.After(until) {
					continue
				}
				out = append(out, DraftCandidate{
					MediaID:    item.MediaID,
					UpdateTime: updated,
					Articles:   snapshotArticles(item.Content.NewsItem),
				})
			}
			if reachedBeforeWindow || int64(len(list.Item)) < reconcilePageSize {
				return nil
			}
		}
		return nil
	})
	if err != nil {
		return nil, err
	}
	return out, nil
}

// GetDraftArticles reads one draft by media id for fingerprint verification.
func (s *Service) GetDraftArticles(mediaID string) ([]DraftArticleSnapshot, error) {
	var snapshots []DraftArticleSnapshot
	err := s.withWechatSDKHTTPClient(func() error {
		dm := s.getOfficialAccount().GetDraft()
		articles, err := dm.GetDraft(mediaID)
		if err != nil {
			return fmt.Errorf("get draft: %w", ExplainDraftError(err))
		}
		for _, article := range articles {
			if article == nil {
				continue
			}
			snapshots = append(snapshots, DraftArticleSnapshot{
				Title:        article.Title,
				Author:       article.Author,
				Digest:       article.Digest,
				Content:      article.Content,
				ThumbMediaID: article.ThumbMediaID,
				ShowCoverPic: article.ShowCoverPic,
			})
		}
		return nil
	})
	if err != nil {
		return nil, err
	}
	return snapshots, nil
}

func snapshotArticles(articles []draft.Article) []DraftArticleSnapshot {
	if len(articles) == 0 {
		return nil
	}
	out := make([]DraftArticleSnapshot, 0, len(articles))
	for _, article := range articles {
		out = append(out, DraftArticleSnapshot{
			Title:        article.Title,
			Author:       article.Author,
			Digest:       article.Digest,
			Content:      article.Content,
			ThumbMediaID: article.ThumbMediaID,
			ShowCoverPic: article.ShowCoverPic,
		})
	}
	return out
}
