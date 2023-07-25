package cli

import (
	"context"
	"os"
	"os/signal"

	"github.com/charmbracelet/log"
)

type Provider interface {
	// Context - creates a context for the command to run in.
	Context() (context.Context, context.CancelFunc)

	// Logger - creates a logger for the command to use.
	Logger() *log.Logger
}

type providerImpl struct {
	logger *log.Logger
}

func Default(debug bool) *providerImpl {
	logLevel := log.InfoLevel
	if debug {
		logLevel = log.DebugLevel
	}
	
	return &providerImpl{
		logger: log.NewWithOptions(os.Stderr, log.Options{
			Level: logLevel,
			ReportTimestamp: false,
		}),
	}
}

var _ Provider = (*providerImpl)(nil)

func (p *providerImpl) Context() (context.Context, context.CancelFunc) {
	// to allow user cancellation via ctrl+c
	return signal.NotifyContext(context.Background(), os.Interrupt)
}

func (p *providerImpl) Logger() *log.Logger {
	return p.logger
}