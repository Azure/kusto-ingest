package testingcli

import (
	"context"
	"io"

	"github.com/Azure/kusto-ingest/internal/cli"
	"github.com/charmbracelet/log"
)

// TestProvider implements cli.Provider for testing usage.
type TestProvider struct {
	ContextFn func() (context.Context, context.CancelFunc)

	LoggerFn func() *log.Logger
}

var _ cli.Provider = (*TestProvider)(nil)

// New creates a testing cli provider with optional mutate functions.
func New(ms ...func(*TestProvider)) *TestProvider {
	rv := &TestProvider{
		ContextFn: func() (context.Context, context.CancelFunc) {
			return context.WithCancel(context.Background())
		},
		LoggerFn: func() *log.Logger {
			return log.New(io.Discard)
		},
	}

	for _, m := range ms {
		m(rv)
	}

	return rv
}

func (tp *TestProvider) Context() (context.Context, context.CancelFunc) {
	return tp.ContextFn()
}

func (tp *TestProvider) Logger() *log.Logger {
	return tp.LoggerFn()
}