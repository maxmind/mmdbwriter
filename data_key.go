package mmdbwriter

import (
	"hash/maphash"

	"github.com/maxmind/mmdbwriter/mmdbtype"
)

// keyWriter is similar to dataWriter but it will never use pointers. This
// will produce a unique key for the type.
type keyWriter struct {
	*maphash.Hash
}

func newKeyWriter() *keyWriter {
	return &keyWriter{Hash: new(maphash.Hash)}
}

// This is just a quick hack. I am sure there is
// something better.
func (kw *keyWriter) key(t mmdbtype.DataType) (uint64, error) {
	kw.Reset()
	_, err := t.WriteTo(kw)
	if err != nil {
		return 0, err
	}
	return kw.Sum64(), nil
}

func (kw *keyWriter) WriteOrWritePointer(t mmdbtype.DataType) (int64, error) {
	return t.WriteTo(kw)
}
