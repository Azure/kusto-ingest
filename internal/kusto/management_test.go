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

func Test_ManagementOptions_invokeQueryWithRetries(t *testing.T) {
	t.Run("success on first attempt", func(t *testing.T) {
		cli := testingcli.New()

		callCount := 0
		invokeQuery := func() error {
			callCount++
			return nil
		}

		opts := ManagementOptions{
			MaxRetries: 3,
			MaxTimeout: 10,
		}

		err := opts.invokeQueryWithRetries(cli, invokeQuery)
		assert.NoError(t, err)
		assert.Equal(t, 1, callCount, "should succeed on first attempt")
	})

	t.Run("retries on transient error then succeeds", func(t *testing.T) {
		cli := testingcli.New()

		callCount := 0
		invokeQuery := func() error {
			callCount++
			if callCount < 3 {
				// Return a retryable timeout error
				return kustoerrors.ES(kustoerrors.OpMgmt, kustoerrors.KTimeout, "request timed out")
			}
			return nil
		}

		opts := ManagementOptions{
			MaxRetries: 5,
			MaxTimeout: 10,
		}

		err := opts.invokeQueryWithRetries(cli, invokeQuery)
		assert.NoError(t, err)
		assert.Equal(t, 3, callCount, "should retry until success")
	})

	t.Run("fails immediately on non-retryable error", func(t *testing.T) {
		cli := testingcli.New()

		callCount := 0
		invokeQuery := func() error {
			callCount++
			// Return a non-retryable client args error
			return kustoerrors.ES(kustoerrors.OpMgmt, kustoerrors.KClientArgs, "invalid query syntax")
		}

		opts := ManagementOptions{
			MaxRetries: 3,
			MaxTimeout: 10,
		}

		err := opts.invokeQueryWithRetries(cli, invokeQuery)
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "non-retryable kusto error")
		assert.Equal(t, 1, callCount, "should not retry on non-retryable errors")
	})

	t.Run("fails after exhausting max retries", func(t *testing.T) {
		cli := testingcli.New()

		callCount := 0
		invokeQuery := func() error {
			callCount++
			// Always return a retryable error
			return kustoerrors.ES(kustoerrors.OpMgmt, kustoerrors.KTimeout, "request timed out")
		}

		opts := ManagementOptions{
			MaxRetries: 2,
			MaxTimeout: 30,
		}

		err := opts.invokeQueryWithRetries(cli, invokeQuery)
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "exhausted max retries (2)")
		assert.Equal(t, 3, callCount, "should attempt MaxRetries+1 times (0, 1, 2)")
	})

	t.Run("respects max timeout", func(t *testing.T) {
		cli := testingcli.New()

		callCount := 0
		invokeQuery := func() error {
			callCount++
			// Always return a retryable error
			return kustoerrors.ES(kustoerrors.OpMgmt, kustoerrors.KTimeout, "request timed out")
		}

		opts := ManagementOptions{
			MaxRetries: 10, // High retry count
			MaxTimeout: 1,  // But very short timeout (1 second)
		}

		err := opts.invokeQueryWithRetries(cli, invokeQuery)
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "max timeout reached")
		// With 1 second timeout and exponential backoff (1s, 2s, 4s...),
		// we should only get a few attempts before timeout
		assert.Less(t, callCount, 5, "should stop early due to timeout")
	})
}
