package kusto

import "fmt"

func (f *FileIngestOptions) Run() error {
	fmt.Printf("%+v\n", f)

	return nil
}