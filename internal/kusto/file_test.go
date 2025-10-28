package kusto

import (
	"context"
	"os"
	"path/filepath"
	"testing"

	"github.com/Azure/azure-kusto-go/kusto/data/errors"
	"github.com/Azure/azure-kusto-go/kusto/ingest"
	"github.com/Azure/kusto-ingest/internal/cli/testingcli"
	"github.com/Azure/kusto-ingest/internal/kusto/testingkusto"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func writeToTestFile(t testing.TB, fileName string, content []byte) string {
	t.Helper()

	tempDir := t.TempDir()
	testFilePath := filepath.Join(tempDir, fileName)
	err := os.WriteFile(testFilePath, content, 0640)
	require.NoError(t, err)

	return testFilePath
}

func Test_FileIngestOptions_FileOptions(t *testing.T) {
	t.Run("with mapping", func(t *testing.T) {
		mappingFile := writeToTestFile(t, "test-mapping.json", []byte(`[]`))

		options := FileIngestOptions{
			Format:       "csv",
			MappingsFile: mappingFile,
		}

		fileOptions, err := options.FileOptions()
		assert.NoError(t, err)
		assert.NotEmpty(t, fileOptions)
	})

	t.Run("with invalid mapping file", func(t *testing.T) {
		options := FileIngestOptions{
			Format:       "csv",
			MappingsFile: "some-random-file",
		}

		_, err := options.FileOptions()
		assert.Error(t, err)
	})

	t.Run("without mapping", func(t *testing.T) {
		options := FileIngestOptions{
			Format: "csv",
		}

		fileOptions, err := options.FileOptions()
		assert.NoError(t, err)
		assert.NotEmpty(t, fileOptions)
	})
}

func Test_FileIngestOptions_Run_IngestFile_NoMapping(t *testing.T) {
	sourceFile := writeToTestFile(t, "logs.json", []byte("{}"))

	cli := testingcli.New()

	ingestor := testingkusto.New(func(ing *testingkusto.Ingestor) {
		ing.FromFileFunc = func(ctx context.Context, fPath string, options ...ingest.FileOption) (*ingest.Result, error) {
			assert.Equal(t, sourceFile, fPath)
			assert.NotEmpty(t, options)

			return &ingest.Result{}, nil
		}
	})

	opts := FileIngestOptions{
		SourceFile:  sourceFile,
		Format:      "multijson",
		Auth:        newTestAuth(),
		KustoTarget: newTestKustoTarget(),

		ingestorBuildSettings: ingestorBuildSettings{
			CreateIngestor: func(target KustoTargetOptions, auth AuthOptions) (ingest.Ingestor, error) {
				return ingestor, nil
			},
		},
	}

	err := opts.Run(cli)
	assert.NoError(t, err)
}

func Test_FileIngestOptions_Run_IngestFile_WithMapping(t *testing.T) {
	sourceFile := writeToTestFile(t, "logs.json", []byte("{}"))
	sourceFileMapping := writeToTestFile(t, "logs-mapping.json", []byte("[]"))

	cli := testingcli.New()

	ingestor := testingkusto.New(func(ing *testingkusto.Ingestor) {
		ing.FromFileFunc = func(ctx context.Context, fPath string, options ...ingest.FileOption) (*ingest.Result, error) {
			assert.Equal(t, sourceFile, fPath)
			assert.NotEmpty(t, options)

			return &ingest.Result{}, nil
		}
	})

	opts := FileIngestOptions{
		SourceFile:   sourceFile,
		Format:       "multijson",
		MappingsFile: sourceFileMapping,
		Auth:         newTestAuth(),
		KustoTarget:  newTestKustoTarget(),

		ingestorBuildSettings: ingestorBuildSettings{
			CreateIngestor: func(target KustoTargetOptions, auth AuthOptions) (ingest.Ingestor, error) {
				return ingestor, nil
			},
		},
	}

	err := opts.Run(cli)
	assert.NoError(t, err)
}

func Test_FileIngestOptions_Run_CreateIngestorError(t *testing.T) {
	sourceFile := writeToTestFile(t, "logs.json", []byte("{}"))
	cli := testingcli.New()

	opts := FileIngestOptions{
		SourceFile:  sourceFile,
		Format:      "multijson",
		Auth:        newTestAuth(),
		KustoTarget: newTestKustoTarget(),
		ingestorBuildSettings: ingestorBuildSettings{
			CreateIngestor: func(target KustoTargetOptions, auth AuthOptions) (ingest.Ingestor, error) {
				return nil, assert.AnError
			},
		},
	}

	err := opts.Run(cli)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "create Kusto ingestor")
}

func Test_FileIngestOptions_invokeIngestWithRetries(t *testing.T) {
	t.Run("success on first attempt", func(t *testing.T) {
		cli := testingcli.New()

		callCount := 0
		invokeIngest := func() error {
			callCount++
			return nil
		}

		opts := FileIngestOptions{
			MaxRetries: 3,
			MaxTimeout: 10,
		}

		err := opts.invokeIngestWithRetries(cli, invokeIngest)
		assert.NoError(t, err)
		assert.Equal(t, 1, callCount, "should succeed on first attempt")
	})

	t.Run("retries on transient error then succeeds", func(t *testing.T) {
		cli := testingcli.New()

		callCount := 0
		invokeIngest := func() error {
			callCount++
			if callCount < 3 {
				// Return a retryable HTTP error
				return errors.ES(errors.OpFileIngest, errors.KHTTPError, "internal server error")
			}
			return nil
		}

		opts := FileIngestOptions{
			MaxRetries: 5,
			MaxTimeout: 10,
		}

		err := opts.invokeIngestWithRetries(cli, invokeIngest)
		assert.NoError(t, err)
		assert.Equal(t, 3, callCount, "should retry until success")
	})

	t.Run("fails immediately on non-retryable error", func(t *testing.T) {
		cli := testingcli.New()

		callCount := 0
		invokeIngest := func() error {
			callCount++
			// Return a non-retryable client args error
			return errors.ES(errors.OpFileIngest, errors.KClientArgs, "invalid arguments")
		}

		opts := FileIngestOptions{
			MaxRetries: 3,
			MaxTimeout: 10,
		}

		err := opts.invokeIngestWithRetries(cli, invokeIngest)
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "non-retryable kusto error")
		assert.Equal(t, 1, callCount, "should not retry on non-retryable errors")
	})

	t.Run("fails after exhausting max retries", func(t *testing.T) {
		cli := testingcli.New()

		callCount := 0
		invokeIngest := func() error {
			callCount++
			// Always return a retryable error
			return errors.ES(errors.OpFileIngest, errors.KHTTPError, "internal server error")
		}

		opts := FileIngestOptions{
			MaxRetries: 2,
			MaxTimeout: 30,
		}

		err := opts.invokeIngestWithRetries(cli, invokeIngest)
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "exhausted max retries (2)")
		assert.Equal(t, 3, callCount, "should attempt MaxRetries+1 times (0, 1, 2)")
	})

	t.Run("respects max timeout", func(t *testing.T) {
		cli := testingcli.New()

		callCount := 0
		invokeIngest := func() error {
			callCount++
			// Always return a retryable error
			return errors.ES(errors.OpFileIngest, errors.KHTTPError, "internal server error")
		}

		opts := FileIngestOptions{
			MaxRetries: 10, // High retry count
			MaxTimeout: 1,  // But very short timeout (1 second)
		}

		err := opts.invokeIngestWithRetries(cli, invokeIngest)
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "max timeout reached")
		// With 1 second timeout and exponential backoff (1s, 2s, 4s...),
		// we should only get a few attempts before timeout
		assert.Less(t, callCount, 5, "should stop early due to timeout")
	})
}
