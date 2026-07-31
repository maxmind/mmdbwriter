package mmdbwriter

import (
	"bytes"
	"crypto/sha256"
	"fmt"
	"hash"
	"hash/maphash"

	"github.com/maxmind/mmdbwriter/v2/mmdbtype"
)

// dataHasher serializes a value into its canonical, pointer-free MMDB encoding
// and hashes it for the in-memory data map. Hash matches are always followed by
// an exact comparison, so correctness does not depend on collision resistance.
type dataHasher struct {
	bytes.Buffer

	seed     maphash.Seed
	hashFunc func([]byte) uint64
}

func newDataHasher() *dataHasher {
	return &dataHasher{seed: maphash.MakeSeed()}
}

func newDataHasherWithHash(hashFunc func([]byte) uint64) *dataHasher {
	return &dataHasher{hashFunc: hashFunc}
}

func (h *dataHasher) Hash(value mmdbtype.DataType) (uint64, error) {
	h.Reset()
	if _, err := value.WriteTo(h); err != nil {
		return 0, err
	}
	if h.hashFunc != nil {
		return h.hashFunc(h.Bytes()), nil
	}
	return maphash.Bytes(h.seed, h.Bytes()), nil
}

func (h *dataHasher) WriteOrWritePointer(value mmdbtype.DataType) (int64, error) {
	return value.WriteTo(h)
}

func (h *dataHasher) WriteOrWritePointerString(value mmdbtype.String) (int64, error) {
	return value.WriteTo(h)
}

// keyWriter is similar to dataWriter but it will never use pointers. This
// will produce a unique key for the type.
type keyWriter struct {
	*bytes.Buffer

	sha256 hash.Hash
	key    [sha256.Size]byte
}

func newKeyWriter() *keyWriter {
	return &keyWriter{Buffer: &bytes.Buffer{}, sha256: sha256.New()}
}

// Key generates a unique key for the data structure v.
//
// This is just a quick hack. I am sure there is
// something better.
func (kw *keyWriter) Key(v mmdbtype.DataType) ([]byte, error) {
	kw.Truncate(0)
	kw.sha256.Reset()
	_, err := v.WriteTo(kw)
	if err != nil {
		return nil, err
	}
	if _, err := kw.WriteTo(kw.sha256); err != nil {
		return nil, fmt.Errorf("writing key to writer: %w", err)
	}
	return kw.sha256.Sum(kw.key[:0]), nil
}

// KeyString is intentionally identical to Key but takes a concrete String to
// keep map-key writes from boxing into DataType on the write hot path.
func (kw *keyWriter) KeyString(v mmdbtype.String) ([]byte, error) {
	kw.Truncate(0)
	kw.sha256.Reset()
	_, err := v.WriteTo(kw)
	if err != nil {
		return nil, err
	}
	if _, err := kw.WriteTo(kw.sha256); err != nil {
		return nil, fmt.Errorf("writing key to writer: %w", err)
	}
	return kw.sha256.Sum(kw.key[:0]), nil
}

func (kw *keyWriter) WriteOrWritePointer(t mmdbtype.DataType) (int64, error) {
	return t.WriteTo(kw)
}

// WriteOrWritePointerString mirrors WriteOrWritePointer without converting the
// map key to DataType. keyWriter never emits pointers.
func (kw *keyWriter) WriteOrWritePointerString(t mmdbtype.String) (int64, error) {
	return t.WriteTo(kw)
}
