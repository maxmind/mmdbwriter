package mmdbwriter

import (
	"bytes"
	"encoding/json"
	"fmt"

	"github.com/pkg/errors"
)

type dataWriter struct {
	buf      *bytes.Buffer
	pointers map[string]int
}

func newDataWriter() *dataWriter {
	return &dataWriter{
		buf:      &bytes.Buffer{},
		pointers: map[string]int{},
	}
}

func (dw *dataWriter) write(t DataType) (int, error) {
	key, err := key(t)
	if err != nil {
		return 0, err
	}

	offset, ok := dw.pointers[key]
	if ok {
		return offset, nil
	}

	offset = dw.buf.Len()
	_, err = t.writeTo(dw.buf)
	if err != nil {
		return 0, err
	}

	dw.pointers[key] = offset

	return offset, nil
}

// This is just a quick hack. I am sure there is
// something better
func key(t DataType) (string, error) {
	bytes, err := json.Marshal(t)
	if err != nil {
		return "", errors.Wrap(err, "error marshalling to JSON")
	}
	return fmt.Sprintf("%d\x00%s", t.typeNum(), bytes), nil
}
