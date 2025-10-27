package kusto

import (
	"context"
	"errors"
	"testing"

	"github.com/Azure/azure-kusto-go/kusto"
	kustoerrors "github.com/Azure/azure-kusto-go/kusto/data/errors"
	"github.com/Azure/azure-kusto-go/kusto/ingest"
	"github.com/Azure/kusto-ingest/internal/cli/testingcli"
	"github.com/Azure/kusto-ingest/internal/kusto/testingkusto"
	"github.com/stretchr/testify/assert"
)

func Test_ManagementOptions_Run_Success(t *testing.T) {
	cli := testingcli.New()

	executed := false
	q := testingkusto.NewQueryClient(func(qc *testingkusto.QueryClient) {
		qc.MgmtFn = func(_ context.Context, db string, stmt kusto.Statement, _ ...kusto.QueryOption) (*kusto.RowIterator, error) {
			executed = true
			assert.Equal(t, "TestDatabase", db)
			return nil, nil
		}
	})

	opts := ManagementOptions{
		Source:      []byte(".show tables"),
		Auth:        newTestAuth(),
		KustoTarget: newTestKustoTarget(),
		ingestorBuildSettings: ingestorBuildSettings{
			CreateQueryClient: func(target KustoTargetOptions, auth AuthOptions) (ingest.QueryClient, error) {
				return q, nil
			},
		},
	}

	err := opts.Run(cli)
	assert.NoError(t, err)
	assert.True(t, executed, "expected management command to execute")
}

func Test_ManagementOptions_Run_CreateClientError(t *testing.T) {
	cli := testingcli.New()

	opts := ManagementOptions{
		Source:      []byte(".show tables"),
		Auth:        newTestAuth(),
		KustoTarget: newTestKustoTarget(),
		ingestorBuildSettings: ingestorBuildSettings{
			CreateQueryClient: func(target KustoTargetOptions, auth AuthOptions) (ingest.QueryClient, error) {
				return nil, errors.New("boom")
			},
		},
	}

	err := opts.Run(cli)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "create Kusto query client")
}

func Test_ManagementOptions_Run_MgmtError(t *testing.T) {
	cli := testingcli.New()

	q := testingkusto.NewQueryClient(func(qc *testingkusto.QueryClient) {
		qc.MgmtFn = func(_ context.Context, db string, stmt kusto.Statement, _ ...kusto.QueryOption) (*kusto.RowIterator, error) {
			return nil, errors.New("mgmt failed")
		}
	})

	opts := ManagementOptions{
		Source:      []byte(".show tables"),
		Auth:        newTestAuth(),
		KustoTarget: newTestKustoTarget(),
		ingestorBuildSettings: ingestorBuildSettings{
			CreateQueryClient: func(target KustoTargetOptions, auth AuthOptions) (ingest.QueryClient, error) {
				return q, nil
			},
		},
	}

	err := opts.Run(cli)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "execute management commands")
}

func Test_ManagementOptions_Run_RetryLogic(t *testing.T) {
	t.Run("retries on transient error", func(t *testing.T) {
		cli := testingcli.New()
		
		callCount := 0
		queryClient := testingkusto.NewQueryClient(func(qc *testingkusto.QueryClient) {
			qc.MgmtFn = func(ctx context.Context, db string, stmt kusto.Statement, options ...kusto.QueryOption) (*kusto.RowIterator, error) {
				callCount++
				if callCount < 2 {
					// Return a retryable timeout error
					return nil, kustoerrors.ES(kustoerrors.OpMgmt, kustoerrors.KTimeout, "request timed out")
				}
				return nil, nil
			}
		})

		opts := ManagementOptions{
			Source:      []byte(".show tables"),
			MaxRetries:  2,  // Fewer retries for faster test
			MaxTimeout:  10, // Shorter timeout for faster test
			Auth:        newTestAuth(),
			KustoTarget: newTestKustoTarget(),
			ingestorBuildSettings: ingestorBuildSettings{
				CreateQueryClient: func(target KustoTargetOptions, auth AuthOptions) (ingest.QueryClient, error) {
					return queryClient, nil
				},
			},
		}

		err := opts.Run(cli)
		assert.NoError(t, err)
		assert.Equal(t, 2, callCount, "should retry on transient errors")
	})

	t.Run("fails immediately on non-retryable error", func(t *testing.T) {
		cli := testingcli.New()
		
		callCount := 0
		queryClient := testingkusto.NewQueryClient(func(qc *testingkusto.QueryClient) {
			qc.MgmtFn = func(ctx context.Context, db string, stmt kusto.Statement, options ...kusto.QueryOption) (*kusto.RowIterator, error) {
				callCount++
				// Return a non-retryable client args error
				return nil, kustoerrors.ES(kustoerrors.OpMgmt, kustoerrors.KClientArgs, "invalid query syntax")
			}
		})

		opts := ManagementOptions{
			Source:      []byte(".show tables"),
			MaxRetries:  2,  // Fewer retries for faster test
			MaxTimeout:  10, // Shorter timeout for faster test
			Auth:        newTestAuth(),
			KustoTarget: newTestKustoTarget(),
			ingestorBuildSettings: ingestorBuildSettings{
				CreateQueryClient: func(target KustoTargetOptions, auth AuthOptions) (ingest.QueryClient, error) {
					return queryClient, nil
				},
			},
		}

		err := opts.Run(cli)
		assert.Error(t, err)
		assert.Equal(t, 1, callCount, "should not retry on non-retryable errors")
	})
}
