package kusto

import (
	"context"
	"os"
	"path/filepath"
	"testing"

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
	t.Run("with mapping", func (t *testing.T)  {
		mappingFile := writeToTestFile(t, "test-mapping.json", []byte(`[]`))

		options := FileIngestOptions{
			Format: "csv",
			MappingsFile: mappingFile,
		}

		fileOptions, err := options.FileOptions()
		assert.NoError(t, err)
		assert.NotEmpty(t, fileOptions)
	})

	t.Run("with invalid mapping file", func(t *testing.T) {
		options := FileIngestOptions{
			Format: "csv",
			MappingsFile: "some-random-file",
		}

		_, err := options.FileOptions()
		assert.Error(t, err)
	})

	t.Run("without mapping", func (t *testing.T)  {
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
		SourceFile: sourceFile,
		Format: "multijson",
		Auth: newTestAuth(),
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
		SourceFile: sourceFile,
		Format: "multijson",
		MappingsFile: sourceFileMapping,
		Auth: newTestAuth(),
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