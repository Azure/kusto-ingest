package kusto

import (
	"github.com/Azure/azure-kusto-go/kusto"
	"github.com/Azure/azure-kusto-go/kusto/ingest"
	"github.com/charmbracelet/log"
)

func createKustoClient(
	target KustoTargetOptions,
	auth AuthOptions,
	logger *log.Logger,
) (*kusto.Client, error) {
	builder := kusto.NewConnectionStringBuilder(target.Endpoint)
	if err := auth.PrepareKustoConnectionStringBuilder(builder, logger); err != nil {
		return nil, err
	}

	return kusto.New(builder)
}

type ingestorBuildSettings struct {
	// Logger - optional logger for diagnostic token logging in default client creation.
	Logger *log.Logger

	// CreateQueryClient - optional callback for creating the ingest query client.
	// Defaults to creating a client via kusto.New.
	CreateQueryClient func(target KustoTargetOptions, auth AuthOptions) (ingest.QueryClient, error)

	// CreateIngestor - optional callback for creating the Kusto ingestor.
	// Defaults to creating an instance via ingest.New.
	CreateIngestor func(target KustoTargetOptions, auth AuthOptions) (ingest.Ingestor, error)
}

func (s ingestorBuildSettings) createQueryClient(
	target KustoTargetOptions,
	auth AuthOptions,
) (ingest.QueryClient, error) {
	if s.CreateQueryClient != nil {
		return s.CreateQueryClient(target, auth)
	}

	client, err := createKustoClient(target, auth, s.Logger)
	if err != nil {
		return nil, err
	}
	return client, nil
}

func (s ingestorBuildSettings) createIngestor(
	target KustoTargetOptions,
	auth AuthOptions,
) (ingest.Ingestor, error) {
	if s.CreateIngestor != nil {
		return s.CreateIngestor(target, auth)
	}

	queryClient, err := s.createQueryClient(target, auth)
	if err != nil {
		return nil, err
	}

	return ingest.New(queryClient, target.Database, target.Table)
}
