package mmdbwriter

import (
	"bytes"

	"github.com/maxmind/mmdbwriter/mmdbtype"
)

// keyWriter is similar to dataWriter but it will never use pointers. This
// will produce a unique key for the type.
type keyWriter struct {
	*bytes.Buffer
}

func newKeyWriter() *keyWriter {
	return &keyWriter{Buffer: &bytes.Buffer{}}
}

// This is just a quick hack. I am sure there is
// something better
func (kw *keyWriter) key(t mmdbtype.DataType) ([]byte, error) {
	kw.Truncate(0)
	_, err := t.WriteTo(kw)
	if err != nil {
		return nil, err
	}
	return kw.Bytes(), nil
}

func (kw *keyWriter) WriteOrWritePointer(t mmdbtype.DataType) (int64, error) {
	return t.WriteTo(kw)
}
