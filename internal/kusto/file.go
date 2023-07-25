package kusto

import (
	"fmt"
	"os"
	"time"

	"github.com/Azure/azure-kusto-go/kusto/ingest"
	"github.com/Azure/kusto-ingest/internal/cli"
)

func (f FileIngestOptions) FileOptions() ([]ingest.FileOption, error) {
	var rv []ingest.FileOption

	ingestDataFormat := f.Format.ToIngestDataFormat()
	if f.MappingsFile != "" {
		mappingsContent, err := os.ReadFile(f.MappingsFile)
		if err != nil {
			return nil, fmt.Errorf("read mappings file %q: %w", f.MappingsFile, err)
		}
		rv = append(rv, ingest.IngestionMapping(mappingsContent, ingestDataFormat))
	} else {
		rv = append(rv, ingest.FileFormat(ingestDataFormat))
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
	)

	fileOptions, err := f.FileOptions()
	if err != nil {
		return err
	}

	ingestor, err := f.ingestorBuildSettings.createIngestor(f.KustoTarget, f.Auth)
	if err != nil {
		return fmt.Errorf("create Kusto ingestor: %w", err)
	}
	defer func() { _ = ingestor.Close() }()

	ctx, cancel := cli.Context()
	defer cancel()

	cli.Logger().Info("file ingestion started")
	start := time.Now()
	_, err = ingestor.FromFile(
		ctx,
		f.SourceFile,
		fileOptions...,
	)
	if err != nil {
		return fmt.Errorf("ingest from file %q: %w", f.SourceFile, err)
	}
	cli.Logger().Info("file ingestion completed", "duration", time.Since(start))

	return nil
}