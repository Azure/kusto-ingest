package kusto

// AuthOptions provides the authenticate configuration for the Kusto client.
type AuthOptions struct {
	TenantID     string `env:"AZURE_TENANT_ID"`
	ClientID     string `env:"AZURE_CLIENT_ID"`
	ClientSecret string `env:"AZURE_CLIENT_SECRET"`
}

// KustoTargetOptions provides the target configuration for the Kusto client.
type KustoTargetOptions struct {
	Endpoint string `required:"" env:"KUSTO_ENDPOINT" help:"The Kusto endpoint to ingest data to."`
	Database string `required:"" env:"KUSTO_DATABASE" help:"The Kusto database to ingest data to."`
	Table    string `required:"" env:"KUSTO_TABLE" help:"The Kusto table to ingest data to."`
}

// FileIngestOptions provides the configuration for ingesting from local file.
type FileIngestOptions struct {
	SourceFile   string `arg:"" required:"" help:"The source file to ingest."`
	MappingsFile string `optional:"" help:"The mappings file to use. Optional"`
	Format       string `optional:"" enum:"json,csv" default:"json" help:"The format of the source file. Default is csv."`

	AuthOptions        `prefix:"auth-"`
	KustoTargetOptions `prefix:"kusto-"`
}
