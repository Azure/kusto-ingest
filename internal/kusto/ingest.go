package kusto

import (
	"github.com/Azure/azure-kusto-go/kusto"
	"github.com/Azure/azure-kusto-go/kusto/ingest"
)

func createKustoClient(
	target KustoTargetOptions,
	auth AuthOptions,
) (*kusto.Client, error) {
	builder := kusto.NewConnectionStringBuilder(target.Endpoint)
	auth.PrepareKustoConnectionStringBuilder(builder)

	return kusto.New(builder)
}

type ingestorBuildSettings struct {
	// CreateQueryClient - optional callback for creating the ingest query client.
	// Defaults to creating a client via kusto.New
	CreateQueryClient func(target KustoTargetOptions, auth AuthOptions) (ingest.QueryClient, error)
}

func (s ingestorBuildSettings) createQueryClient(
	target KustoTargetOptions,
	auth AuthOptions,
) (ingest.QueryClient, error) {
	if s.CreateQueryClient != nil {
		return s.CreateQueryClient(target, auth)
	}

	client, err := createKustoClient(target, auth)
	if err != nil {
		return nil, err
	}
	return client, nil
}

func createIngestor(
	target KustoTargetOptions,
	auth AuthOptions,
	settings ingestorBuildSettings, // reserved for unit testing
) (ingest.Ingestor, error) {
	queryClient, err := settings.createQueryClient(target, auth)
	if err != nil {
		return nil, err
	}

	return ingest.New(queryClient, target.Database, target.Table)
}