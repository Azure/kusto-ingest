package testingkusto

import (
	"context"
	"io"

	"github.com/Azure/azure-kusto-go/kusto/ingest"
)

// Ingestor is a fake implementation of ingest.Ingestor.
type Ingestor struct {
	CloseFn func() error
	FromFileFunc func(ctx context.Context, fPath string, options ...ingest.FileOption) (*ingest.Result, error)
	FromReaderFunc func(ctx context.Context, reader io.Reader, options ...ingest.FileOption) (*ingest.Result, error)
}

var _ ingest.Ingestor = (*Ingestor)(nil)

func (ing *Ingestor) Close() error {
	if ing.CloseFn != nil {
		return ing.CloseFn()
	}

	return nil
}

func (ing *Ingestor) FromFile(
	ctx context.Context,
	fPath string,
	options ...ingest.FileOption,
) (*ingest.Result, error) {
	if ing.FromFileFunc != nil {
		return ing.FromFileFunc(ctx, fPath, options...)
	}

	return &ingest.Result{}, nil
}

func (ing *Ingestor) FromReader(
	ctx context.Context,
	reader io.Reader,
	options ...ingest.FileOption,
) (*ingest.Result, error) {
	if ing.FromReaderFunc != nil {
		return ing.FromReaderFunc(ctx, reader, options...)
	}

	return &ingest.Result{}, nil
}

// New creates a ingestor instance.
func New(mu ...func(*Ingestor)) *Ingestor {
	rv := &Ingestor{}
	for _, m := range mu {
		m(rv)
	}

	return rv
}