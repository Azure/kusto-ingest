package kusto

import "fmt"

func (f *FileIngestOptions) Run() error {
	fmt.Printf("%+v\n", f)
	fmt.Println(f.Format.ToIngestDataFormat().CamelCase())

	return nil
}