package saga

import (
	"fmt"
	"strings"
)

// Severity levels for manual checklist entries.
const (
	SeverityManualReview = "manual_review"
	SeverityRetry        = "retry"
)

// DeriveStatus computes the tri-state operation status from replayed steps.
func DeriveStatus(op *Operation) OpStatus {
	if op == nil {
		return OpStatusPartial
	}
	if len(op.Order) == 0 {
		// Started but no side effect was planned/recorded: safe to restart.
		return OpStatusPartial
	}
	hasSuccess := false
	for _, id := range op.Order {
		switch op.Steps[id].Status {
		case StepStatusUnknown:
			return OpStatusUnknown
		case StepStatusSucceeded:
			hasSuccess = true
		}
	}
	if hasSuccess {
		for _, id := range op.Order {
			if op.Steps[id].Status != StepStatusSucceeded {
				return OpStatusPartial
			}
		}
		return OpStatusCompleted
	}
	return OpStatusPartial
}

// BuildReport renders the observable report for an operation.
func BuildReport(op *Operation, journalPath string) *Report {
	if op == nil {
		return nil
	}
	report := &Report{
		OperationID: op.ID,
		Kind:        op.Kind,
		Status:      DeriveStatus(op),
		InputDigest: op.InputDigest,
		JournalPath: journalPath,
		Steps:       make([]StepReport, 0, len(op.Order)),
	}
	for _, id := range op.Order {
		step := op.Steps[id]
		entry := StepReport{
			ID:             step.ID,
			Kind:           step.Kind,
			Status:         step.Status,
			IdempotencyKey: step.IdempotencyKey,
			InputDigest:    step.InputDigest,
			Effect:         step.Effect,
			PostCondition:  step.PostCondition,
			Compensation:   step.Compensation,
			Attempts:       len(step.Attempts),
			VerifiedMethod: step.VerifiedMethod,
			UpdatedAt:      step.UpdatedAt,
		}
		if last := step.LastAttempt(); last != nil {
			entry.LastError = last.Error
			entry.Ambiguous = last.Ambiguous
		}
		report.Steps = append(report.Steps, entry)

		switch step.Status {
		case StepStatusUnknown:
			detail := fmt.Sprintf("步骤 %s (%s) 的远端结果未知（请求可能已成功但响应丢失），禁止盲目重试。", step.ID, step.Kind)
			if last := step.LastAttempt(); last != nil && last.Error != "" {
				detail += " 最后一次错误: " + last.Error
			}
			detail += fmt.Sprintf(" 可执行 `md2wechat saga reconcile %s` 查询对账，或在公众号后台人工核对。", op.ID)
			action := ManualAction{
				StepID:   step.ID,
				Severity: SeverityManualReview,
				Action:   "reconcile_or_manual_review",
				Detail:   detail,
			}
			if step.Compensation != nil {
				action.Compensation = step.Compensation
			}
			report.ManualActions = append(report.ManualActions, action)
		case StepStatusFailed:
			detail := fmt.Sprintf("步骤 %s (%s) 确定性失败，修复原因后可安全重试。", step.ID, step.Kind)
			if last := step.LastAttempt(); last != nil && last.Error != "" {
				detail += " 错误: " + last.Error
			}
			detail += fmt.Sprintf(" 重试: `md2wechat saga resume %s`。", op.ID)
			report.ManualActions = append(report.ManualActions, ManualAction{
				StepID:   step.ID,
				Severity: SeverityRetry,
				Action:   "fix_and_resume",
				Detail:   detail,
			})
		case StepStatusPending:
			report.ManualActions = append(report.ManualActions, ManualAction{
				StepID:   step.ID,
				Severity: SeverityRetry,
				Action:   "resume",
				Detail: fmt.Sprintf("步骤 %s (%s) 尚未执行，继续操作: `md2wechat saga resume %s`。",
					step.ID, step.Kind, op.ID),
			})
		}
	}
	if len(op.Order) == 0 {
		report.ManualActions = append(report.ManualActions, ManualAction{
			StepID:   "",
			Severity: SeverityRetry,
			Action:   "resume",
			Detail:   fmt.Sprintf("操作在执行任何远端副作用前中断，可直接重试原命令，或执行 `md2wechat saga resume %s`。", op.ID),
		})
	}
	return report
}

// FormatReport renders a human-readable summary for non-JSON output.
func FormatReport(report *Report) string {
	if report == nil {
		return ""
	}
	var b strings.Builder
	fmt.Fprintf(&b, "saga 操作 %s: %s\n", report.OperationID, report.Status)
	fmt.Fprintf(&b, "journal: %s\n", report.JournalPath)
	if len(report.Steps) == 0 {
		b.WriteString("步骤: (无)\n")
	}
	for _, step := range report.Steps {
		remote := "-"
		if step.Effect != nil && step.Effect.RemoteID != "" {
			remote = step.Effect.RemoteID
		}
		extra := ""
		if step.VerifiedMethod != "" {
			extra = " (" + step.VerifiedMethod + ")"
		}
		fmt.Fprintf(&b, "  - %s [%s] remote_id=%s%s\n", step.ID, step.Status, remote, extra)
	}
	if len(report.ManualActions) > 0 {
		b.WriteString("人工处理清单:\n")
		for _, item := range report.ManualActions {
			fmt.Fprintf(&b, "  [%s] %s\n", item.Severity, item.Detail)
			if item.Compensation != nil {
				fmt.Fprintf(&b, "         补偿动作（确认重复后手动执行）: %s %s\n",
					item.Compensation.Kind, item.Compensation.RemoteID)
			}
		}
	}
	return strings.TrimRight(b.String(), "\n")
}
