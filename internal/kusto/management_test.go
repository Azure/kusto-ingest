package kusto

import (
	"context"
	"errors"
	"testing"

	"github.com/Azure/azure-kusto-go/kusto"
	"github.com/Azure/azure-kusto-go/kusto/ingest"
	"github.com/Azure/kusto-ingest/internal/cli/testingcli"
	"github.com/Azure/kusto-ingest/internal/kusto/testingkusto"
	"github.com/stretchr/testify/assert"
)

func Test_ManagementOptions_Run_Success(t *testing.T) {
	cli := testingcli.New()

	executed := false
	q := testingkusto.NewQueryClient(func(qc *testingkusto.QueryClient) {
		qc.MgmtFn = func(_ context.Context, db string, stmt kusto.Statement, _ ...kusto.MgmtOption) (*kusto.RowIterator, error) {
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
		qc.MgmtFn = func(_ context.Context, db string, stmt kusto.Statement, _ ...kusto.MgmtOption) (*kusto.RowIterator, error) {
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
