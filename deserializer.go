package mmdbwriter

import (
	"math/big"

	"github.com/maxmind/mmdbwriter/mmdbtype"
	"github.com/pkg/errors"
)

// Potentially, it would make sense to add this to mmdbtypes and make
// it public, but we should wait until the API stabilized here and in
// maxminddb first.

type deserializer struct {
	stack []mmdbtype.DataType
	rv    mmdbtype.DataType
	key   *mmdbtype.String
}

func (d *deserializer) StartSlice(size uint) error {
	return d.add(make(mmdbtype.Slice, 0, size))
}

func (d *deserializer) StartMap(size uint) error {
	return d.add(make(mmdbtype.Map, size))
}

func (d *deserializer) End() error {
	if len(d.stack) == 0 {
		return errors.New("received an End but the stack in empty")
	}
	d.stack = d.stack[:len(d.stack)-1]
	return nil
}

func (d *deserializer) String(v string) error {
	return d.add(mmdbtype.String(v))
}

func (d *deserializer) Float64(v float64) error {
	return d.add(mmdbtype.Float64(v))
}

func (d *deserializer) Bytes(v []byte) error {
	return d.add(mmdbtype.Bytes(v))
}

func (d *deserializer) Uint16(v uint16) error {
	return d.add(mmdbtype.Uint16(v))
}

func (d *deserializer) Uint32(v uint32) error {
	return d.add(mmdbtype.Uint32(v))
}

func (d *deserializer) Int32(v int32) error {
	return d.add(mmdbtype.Int32(v))
}

func (d *deserializer) Uint64(v uint64) error {
	return d.add(mmdbtype.Uint64(v))
}

func (d *deserializer) Uint128(v *big.Int) error {
	t := mmdbtype.Uint128(*v)
	return d.add(&t)
}

func (d *deserializer) Bool(v bool) error {
	return d.add(mmdbtype.Bool(v))
}

func (d *deserializer) Float32(v float32) error {
	return d.add(mmdbtype.Float32(v))
}

func (d *deserializer) add(v mmdbtype.DataType) error {
	if len(d.stack) == 0 {
		d.rv = v
	} else {
		switch parent := d.stack[len(d.stack)-1].(type) {
		case mmdbtype.Map:
			if d.key == nil {
				key, ok := v.(mmdbtype.String)
				if !ok {
					return errors.Errorf("expected a String Map key but received %T", v)
				}
				d.key = &key
				return nil
			}
			parent[*d.key] = v
			d.key = nil
		case mmdbtype.Slice:
			d.stack[len(d.stack)-1] = append(parent, v)
		default:
		}
	}

	switch v := v.(type) {
	case mmdbtype.Map, mmdbtype.Slice:
		d.stack = append(d.stack, v)
	default:
	}
	return nil
}

func (d *deserializer) clear() {
	d.rv = nil

	// Although these shouldn't be necessary normally, they could be needed
	// if we are recovering from an error.
	d.key = nil
	if len(d.stack) > 0 {
		d.stack = d.stack[:0]
	}
}
