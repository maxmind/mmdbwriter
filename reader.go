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
	types []mmdbtype.DataType
}

func newDeserializer() *deserializer {
	return &deserializer{
		types: []mmdbtype.DataType{},
	}
}

func (d *deserializer) StartSlice(size uint) error {
	d.types = append(d.types, make(mmdbtype.Slice, 0, size))
	return nil
}

func (d *deserializer) StartMap(size uint) error {
	d.types = append(d.types, make(mmdbtype.Map, size))
	return nil
}

func (d *deserializer) End() error {
	d.types = append(d.types, nil)
	return nil
}

func (d *deserializer) String(v string) error {
	d.types = append(d.types, mmdbtype.String(v))
	return nil
}

func (d *deserializer) Float64(v float64) error {
	d.types = append(d.types, mmdbtype.Float64(v))
	return nil
}

func (d *deserializer) Bytes(v []byte) error {
	d.types = append(d.types, mmdbtype.Bytes(v))
	return nil
}

func (d *deserializer) Uint16(v uint16) error {
	d.types = append(d.types, mmdbtype.Uint16(v))
	return nil
}

func (d *deserializer) Uint32(v uint32) error {
	d.types = append(d.types, mmdbtype.Uint32(v))
	return nil
}

func (d *deserializer) Int32(v int32) error {
	d.types = append(d.types, mmdbtype.Int32(v))
	return nil
}

func (d *deserializer) Uint64(v uint64) error {
	d.types = append(d.types, mmdbtype.Uint64(v))
	return nil
}

func (d *deserializer) Uint128(v *big.Int) error {
	t := mmdbtype.Uint128(*v)
	d.types = append(d.types, &t)
	return nil
}

func (d *deserializer) Bool(v bool) error {
	d.types = append(d.types, mmdbtype.Bool(v))
	return nil
}

func (d *deserializer) Float32(v float32) error {
	d.types = append(d.types, mmdbtype.Float32(v))
	return nil
}

func (d *deserializer) build() (mmdbtype.DataType, error) {
	t, _, err := buildType(d.types)

	return t, err
}

func buildType(remaining []mmdbtype.DataType) (mmdbtype.DataType, []mmdbtype.DataType, error) {
	if len(remaining) == 0 {
		// This would be a programming bug on our part or in maxminddb.
		return nil, nil, errors.New("unexpected end of data types when deserializing mmdbtype.DataType")
	}
	current := remaining[0]
	remaining = remaining[1:]
	switch current := current.(type) {
	case mmdbtype.Map:
		for {
			var err error
			var key, value mmdbtype.DataType
			key, remaining, err = buildType(remaining)
			if err != nil {
				return nil, nil, err
			}
			if key == nil {
				break
			}
			keyStr, ok := key.(mmdbtype.String)
			if !ok {
				return nil, nil, errors.Errorf("unexpected Map key type: %T", key)
			}
			value, remaining, err = buildType(remaining)
			if err != nil {
				return nil, nil, err
			}
			current[keyStr] = value
		}
	case mmdbtype.Slice:
		i := 0
		for {
			var err error
			var value mmdbtype.DataType
			value, remaining, err = buildType(remaining)
			if err != nil {
				return nil, nil, err
			}
			if value == nil {
				break
			}
			current[i] = value
			i++
		}
	default:
	}
	return current, remaining, nil
}
