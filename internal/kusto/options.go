package kusto

// AuthOptions provides the authenticate configuration for the Kusto client.
// TODO: add support for MSI based authentication.
type AuthOptions struct {
	TenantID     string `env:"AZURE_TENANT_ID" help:"The Azure tenant ID."`
	ClientID     string `env:"AZURE_CLIENT_ID" help:"The Azure client ID."`
	ClientSecret string `env:"AZURE_CLIENT_SECRET" help:"The Azure client secret."`

	AZCLI bool `env:"AZURE_CLI" help:"Use Azure CLI for authentication."`

	ManagedIdentityResourceID string `env:"AZURE_MANAGED_IDENTITY_RESOURCE_ID" help:"The Azure managed identity resource ID."`
}

// KustoTargetOptions provides the target configuration for the Kusto client.
type KustoTargetOptions struct {
	Endpoint string `required:"" env:"KUSTO_ENDPOINT" help:"The Kusto endpoint to ingest data to."`
	Database string `required:"" env:"KUSTO_DATABASE" help:"The Kusto database to ingest data to."`
	Table    string `required:"" env:"KUSTO_TABLE" help:"The Kusto table to ingest data to."`
}

// FileIngestOptions provides the configuration for ingesting from local file.
type FileIngestOptions struct {
	SourceFile   string           `arg:"" type:"existingfile" required:"" help:"The source file to ingest."`
	MappingsFile string           `optional:"" type:"existingfile" help:"The mappings file to use. Optional"`
	Format       DataFormatString `optional:"" enum:"multijson,json,csv" default:"multijson" help:"The format of the source file. Default is multijson."`

	Auth        AuthOptions        `embed:"" prefix:"auth-"`
	KustoTarget KustoTargetOptions `embed:"" prefix:"kusto-"`

	// Retry and timeout configuration
	MaxRetries int `optional:"" default:"3" help:"Maximum number of retries for transient errors (default: 3)."`
	MaxTimeout int `optional:"" default:"60" help:"Maximum timeout in seconds for all retries (default: 60)."`

	// for unit test
	ingestorBuildSettings `kong:"-"`
}

// ManagementOptions provides the configuration for management commands.
type ManagementOptions struct {
	Source []byte `arg:"" type:"filecontent" required:"" help:"The source file to execute."`

	Auth        AuthOptions        `embed:"" prefix:"auth-"`
	KustoTarget KustoTargetOptions `embed:"" prefix:"kusto-"`

	// Retry and timeout configuration
	MaxRetries int `optional:"" default:"3" help:"Maximum number of retries for transient errors (default: 3)."`
	MaxTimeout int `optional:"" default:"60" help:"Maximum timeout in seconds for all retries (default: 60)."`

	// for unit test
	ingestorBuildSettings `kong:"-"`
}
