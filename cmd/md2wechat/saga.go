package main

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"time"

	"github.com/geekjourneyx/md2wechat-skill/internal/converter"
	"github.com/geekjourneyx/md2wechat-skill/internal/saga"
	"github.com/geekjourneyx/md2wechat-skill/internal/wechat"
	"github.com/spf13/cobra"
)

const codeSagaManualActionRequired = "SAGA_MANUAL_ACTION_REQUIRED"

var sagaCmd = &cobra.Command{
	Use:   "saga",
	Short: "Inspect and recover journaled publish operations",
	Long: `The append-only saga journal records every WeChat side effect (material
upload, draft creation) with its idempotency key, remote id, compensation
action, and verifiable postcondition.

Subcommands:
  list                 List journaled operations, newest first
  status [operation]   Show one operation (default: newest) and its manual checklist
  resume <operation>   Re-run the recorded convert, skipping verified steps
  reconcile <operation> Query WeChat to resolve steps whose result was lost`,
	PreRunE: func(cmd *cobra.Command, args []string) error {
		return initConfig()
	},
}

var sagaListCmd = &cobra.Command{
	Use:   "list",
	Short: "List journaled publish operations",
	Args:  cobra.NoArgs,
	RunE: func(cmd *cobra.Command, args []string) error {
		return runSagaList()
	},
}

var sagaStatusCmd = &cobra.Command{
	Use:   "status [operation]",
	Short: "Show one saga operation and its manual-action checklist",
	Args:  cobra.MaximumNArgs(1),
	RunE: func(cmd *cobra.Command, args []string) error {
		return runSagaStatus(args)
	},
}

var sagaResumeCmd = &cobra.Command{
	Use:   "resume <operation>",
	Short: "Resume a journaled publish operation without repeating verified effects",
	Args:  cobra.ExactArgs(1),
	RunE: func(cmd *cobra.Command, args []string) error {
		return runSagaResume(args[0])
	},
}

var sagaReconcileCmd = &cobra.Command{
	Use:   "reconcile <operation>",
	Short: "Query WeChat to resolve steps with unknown remote results",
	Args:  cobra.ExactArgs(1),
	RunE: func(cmd *cobra.Command, args []string) error {
		return runSagaReconcile(args[0])
	},
}

func init() {
	sagaCmd.PersistentFlags().StringVar(&wechatAccountName, "wechat-account", "", "Named WeChat account from config")
	sagaCmd.AddCommand(sagaListCmd, sagaStatusCmd, sagaResumeCmd, sagaReconcileCmd)
}

func runSagaList() error {
	items, err := newSagaStore().List()
	if err != nil {
		return wrapCLIError(codeConvertFailed, err, fmt.Sprintf("list saga operations: %v", err))
	}
	if jsonOutput {
		responseSuccessWith("SAGA_LISTED", "Saga operations", items)
		return nil
	}
	if len(items) == 0 {
		fmt.Fprintln(os.Stderr, "No saga operations found.")
		return nil
	}
	for _, item := range items {
		fmt.Printf("%s  %-9s  steps %d/%d  %s\n",
			item.OperationID, item.Status, item.StepDone, item.StepTotal, item.UpdatedAt.Format(time.RFC3339))
	}
	return nil
}

func runSagaStatus(args []string) error {
	store := newSagaStore()
	var opID string
	if len(args) == 1 {
		opID = args[0]
	} else {
		op, err := store.FindLatest()
		if err != nil {
			return wrapSagaNotFound(err)
		}
		opID = op.ID
	}
	op, journalPath, err := store.ReopenForRead(opID)
	if err != nil {
		return wrapSagaNotFound(err)
	}
	report := saga.BuildReport(op, journalPath)
	return emitSagaReport(report)
}

func runSagaResume(opID string) error {
	store := newSagaStore()
	op, err := store.Open(opID)
	if err != nil {
		return wrapSagaNotFound(err)
	}
	descriptor, err := decodeSagaRequest(op)
	if err != nil {
		return err
	}
	if err := verifySagaRequestSource(descriptor); err != nil {
		return err
	}
	if err := applySagaRequestToConvertFlags(opID, descriptor); err != nil {
		return err
	}
	return runConvert(convertCmd, []string{descriptor.SourcePath})
}

func runSagaReconcile(opID string) error {
	if err := prepareWeChatSideEffect(); err != nil {
		return err
	}
	store := newSagaStore()
	journal, err := store.Resume(opID, "", "", time.Now)
	if err != nil {
		return wrapSagaNotFound(err)
	}
	defer func() { _ = journal.Close() }()

	reconciler := newWechatSagaReconciler(wechat.NewService(cfg, log), journal.Op())
	exec := saga.NewExecutor(store, journal, reconciler)
	if _, err := exec.ReconcileUnknown(context.Background()); err != nil {
		return wrapCLIError(codeConvertFailed, err, fmt.Sprintf("reconcile saga: %v", err))
	}
	status, err := exec.Finish()
	if err != nil {
		return wrapCLIError(codeConvertFailed, err, fmt.Sprintf("finalize saga: %v", err))
	}
	report := exec.Report()
	if err := emitSagaReport(report); err != nil {
		return err
	}
	if status == saga.OpStatusCompleted {
		return nil
	}
	return nil
}

func emitSagaReport(report *saga.Report) error {
	if jsonOutput {
		if len(report.ManualActions) > 0 || report.Status != saga.OpStatusCompleted {
			responseActionRequiredWith(codeSagaManualActionRequired, "Saga operation needs attention", report)
			return nil
		}
		responseSuccessWith("SAGA_COMPLETED", "Saga operation completed", report)
		return nil
	}
	fmt.Println(saga.FormatReport(report))
	return nil
}

func decodeSagaRequest(op *saga.Operation) (sagaPublishRequest, error) {
	var descriptor sagaPublishRequest
	if len(op.Request) == 0 {
		return descriptor, newCLIError(codeConvertInvalid, fmt.Sprintf("saga operation %s has no recorded request", op.ID))
	}
	if err := json.Unmarshal(op.Request, &descriptor); err != nil {
		return descriptor, wrapCLIError(codeConvertInvalid, err, fmt.Sprintf("parse saga request: %v", err))
	}
	return descriptor, nil
}

// verifySagaRequestSource re-reads the source file and proves its body is
// byte-identical to the digest recorded when the operation began.
func verifySagaRequestSource(descriptor sagaPublishRequest) error {
	if descriptor.SourcePath == "" {
		return newCLIError(codeConvertInvalid, "saga request has no source path")
	}
	markdown, err := os.ReadFile(descriptor.SourcePath)
	if err != nil {
		return wrapCLIError(codeConvertReadFailed, err, fmt.Sprintf("read markdown file: %v", err))
	}
	document := converter.ParseArticleDocument(string(markdown))
	bodyDigest := saga.DigestBytes([]byte(document.Body))
	if bodyDigest != descriptor.MarkdownDigest {
		return newCLIError(codeConvertInvalid, fmt.Sprintf(
			"source file changed since operation started (digest %s != recorded %s); start a new operation instead of resuming",
			bodyDigest[:12], descriptor.MarkdownDigest[:12]))
	}
	return nil
}

// applySagaRequestToConvertFlags restores the exact convert invocation recorded
// in the journal. Credentials are intentionally not restored from the journal;
// they come from config/environment as in a normal invocation.
func applySagaRequestToConvertFlags(opID string, req sagaPublishRequest) error {
	convertOperationID = opID
	convertMode = req.Mode
	if convertMode == "" {
		convertMode = "api"
	}
	if err := convertCmd.Flags().Set("theme", req.Theme); err != nil {
		return err
	}
	convertFontSize = req.FontSize
	convertBackgroundType = req.BackgroundType
	convertCustomPrompt = req.CustomPrompt
	convertTitle = req.Title
	convertAuthor = req.Author
	convertDigest = req.Digest
	convertOutput = req.OutputFile
	convertSaveDraft = req.SaveDraftPath
	convertCoverImage = req.CoverImagePath
	convertCoverMediaID = req.CoverMediaID
	convertUpload = req.Upload
	convertDraft = req.CreateDraft
	convertPreview = false
	convertAPIKey = ""
	wechatAccountName = req.Account
	return nil
}

func wrapSagaNotFound(err error) error {
	if err == nil {
		return nil
	}
	return wrapCLIError(codeConvertInvalid, err, err.Error())
}
