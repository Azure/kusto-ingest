package kusto

import (
	"fmt"
	"os"
	"time"

	"github.com/Azure/azure-kusto-go/kusto/data/errors"
	"github.com/Azure/azure-kusto-go/kusto/ingest"
	"github.com/Azure/kusto-ingest/internal/cli"
)

func (f FileIngestOptions) FileOptions() ([]ingest.FileOption, error) {
	var rv []ingest.FileOption

	fileFormat := f.Format.ToIngestDataFormat()
	if f.MappingsFile != "" {
		mappingsContent, err := os.ReadFile(f.MappingsFile)
		if err != nil {
			return nil, fmt.Errorf("read mappings file %q: %w", f.MappingsFile, err)
		}

		// when input format is multijson, we need to set the mapping format to json
		// refs:
		// - https://learn.microsoft.com/en-us/azure/data-explorer/ingestion-supported-formats
		// - https://github.com/Azure/azure-kusto-go/blob/2ff486159db0752e13504a58d67fc298e7b61691/kusto/ingest/internal/properties/properties.go#L55
		ingestDataFormat := fileFormat
		if ingestDataFormat == ingest.MultiJSON {
			ingestDataFormat = ingest.JSON
		}
		rv = append(rv, ingest.IngestionMapping(mappingsContent, ingestDataFormat))
	} else {
		rv = append(rv, ingest.FileFormat(fileFormat))
	}

	return rv, nil
}

func (f FileIngestOptions) Run(cli cli.Provider) error {
	cli.Logger().Debug(
		"file ingestion settings",
		"source", f.SourceFile,
		"format", f.Format,
		"mappings", f.MappingsFile,
		"target.endpoint", f.KustoTarget.Endpoint,
		"target.database", f.KustoTarget.Database,
		"target.table", f.KustoTarget.Table,
		"auth.tenant", f.Auth.TenantID,
		"auth.clientID", f.Auth.ClientID,
		"maxRetries", f.MaxRetries,
		"maxTimeout", f.MaxTimeout,
	)

	fileOptions, err := f.FileOptions()
	if err != nil {
		return err
	}

	ingestor, err := f.createIngestor(f.KustoTarget, f.Auth)
	if err != nil {
		return fmt.Errorf("create Kusto ingestor: %w", err)
	}
	defer func() { _ = ingestor.Close() }()

	ctx, cancel := cli.Context()
	defer cancel()

	invokeIngest := func() error {
		_, err := ingestor.FromFile(ctx, f.SourceFile, fileOptions...)
		return err
	}

	cli.Logger().Info("file ingestion started")
	err = f.invokeIngestWithRetries(cli, invokeIngest)
	if err != nil {
		cli.Logger().Error("failed to ingest file", "error", err)
	}
	return err
}

func (f FileIngestOptions) invokeIngestWithRetries(cli cli.Provider, invokeIngest func() error) error {
	start := time.Now()
	var err error
	baseDelay := 1 * time.Second
	maxTimeout := time.Duration(f.MaxTimeout) * time.Second
	deadline := time.Now().Add(maxTimeout)

	for attempt := 0; attempt <= f.MaxRetries; attempt++ {
		err = invokeIngest()
		if err == nil {
			cli.Logger().Info("file ingestion completed successfully", "duration", time.Since(start), "attempt", attempt+1)
			return nil
		}

		// Check error type for retry logic using azure-kusto-go SDK's Retry function
		if !errors.Retry(err) {
			return fmt.Errorf("non-retryable kusto error: %w", err)
		}

		// Calculate next backoff duration with exponential backoff and jitter
		backoffDelay := exponentialBackoffWithJitter(attempt, baseDelay)

		if time.Now().Add(backoffDelay).After(deadline) {
			return fmt.Errorf("max timeout reached after %d retries: %w", attempt, err)
		}

		cli.Logger().Warn("transient kusto error, will retry", "error", err, "attempt", attempt+1, "backoff", backoffDelay)
		time.Sleep(backoffDelay)
	}

	return fmt.Errorf("exhausted max retries (%d): %w", f.MaxRetries, err)
}
