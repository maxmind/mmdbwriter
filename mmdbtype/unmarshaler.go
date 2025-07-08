package mmdbtype

import (
	"github.com/oschwald/maxminddb-golang/v2/mmdbdata"
)

// Unmarshaler implements the mmdbdata.Unmarshaler interface for converting
// MMDB data back into mmdbtype.DataType values. This is used when loading
// existing MMDB files to reconstruct the original data structures.
type Unmarshaler struct {
	cache  map[uint]DataType
	result DataType
}

// NewUnmarshaler creates a new Unmarshaler for converting MMDB data to mmdbtype values.
func NewUnmarshaler() *Unmarshaler {
	return &Unmarshaler{
		cache: map[uint]DataType{},
	}
}

// UnmarshalMaxMindDB implements the mmdbdata.Unmarshaler interface.
func (u *Unmarshaler) UnmarshalMaxMindDB(decoder *mmdbdata.Decoder) error {
	offset := decoder.Offset()
	if cached, ok := u.cache[offset]; ok {
		u.result = cached
		return nil
	}

	value, err := decodeDataTypeValue(decoder)
	if err != nil {
		return err
	}

	u.cache[offset] = value
	u.result = value
	return nil
}

// Clear resets the unmarshaler state for reuse.
func (u *Unmarshaler) Clear() {
	u.result = nil
}

// Result returns the final unmarshaled value.
func (u *Unmarshaler) Result() DataType {
	return u.result
}
