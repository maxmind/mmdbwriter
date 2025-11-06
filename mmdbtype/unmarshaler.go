package mmdbtype

import (
	"github.com/oschwald/maxminddb-golang/v2/mmdbdata"
)

// Unmarshaler implements the mmdbdata.Unmarshaler interface for converting
// MMDB data back into mmdbtype.DataType values. This is used when loading
// existing MMDB files to reconstruct the original data structures.
//
// The Unmarshaler caches decoded complex types (Map, Slice, Uint128) at all
// nesting levels to improve performance when loading databases with shared
// nested data structures. Simple scalar types are not cached as they are
// cheap to decode.
//
// The zero value of Unmarshaler is safe to use and will unmarshal data
// without caching. Use NewUnmarshaler() to create an Unmarshaler with
// caching enabled for better performance when loading full databases.
type Unmarshaler struct {
	cache  map[uint]DataType
	result DataType
}

// NewUnmarshaler creates a new Unmarshaler with caching enabled for converting
// MMDB data to mmdbtype values. The cache improves performance when loading
// databases with shared data structures by avoiding redundant decoding.
func NewUnmarshaler() *Unmarshaler {
	return &Unmarshaler{
		cache: map[uint]DataType{},
	}
}

// UnmarshalMaxMindDB implements the mmdbdata.Unmarshaler interface.
func (u *Unmarshaler) UnmarshalMaxMindDB(decoder *mmdbdata.Decoder) error {
	value, err := decodeDataTypeValue(decoder, u.cache)
	if err != nil {
		return err
	}

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
