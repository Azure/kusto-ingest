package main

import (
	"github.com/Azure/kusto-ingest/internal/kusto"
	"github.com/alecthomas/kong"
)

var CLI struct {
	File kusto.FileIngestOptions `cmd:"" help:"Ingest data from local file."`
}

func main() {
	ctx := kong.Parse(&CLI)
	err := ctx.Run()
	ctx.FatalIfErrorf(err)
}