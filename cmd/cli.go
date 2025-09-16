package cmd

import (
	"github.com/Azure/kusto-ingest/internal/cli"
	"github.com/Azure/kusto-ingest/internal/kusto"
	"github.com/alecthomas/kong"
)

// CLI is the command line interface structure.
var CLI struct {
	Verbose bool `short:"v" help:"Enable verbose logging."`

	File kusto.FileIngestOptions `cmd:"" help:"Ingest data from local file."`
}

// Main is the entry point for the CLI application.
func Main() {
	ctx := kong.Parse(&CLI)

	ctx.BindTo(cli.Default(CLI.Verbose), (*cli.Provider)(nil))

	err := ctx.Run()
	ctx.FatalIfErrorf(err)
}
