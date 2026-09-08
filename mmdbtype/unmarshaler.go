package mmdbtype

import (
	"github.com/oschwald/maxminddb-golang/v2/mmdbdata"
)

// Unmarshaler implements the mmdbdata.CursorUnmarshaler interface for converting
// MMDB data back into mmdbtype.DataType values. This is used when loading
// existing MMDB files to reconstruct the original data structures.
//
// The Unmarshaler caches decoded containers (Map and Slice) at all nesting
// levels to improve performance when loading databases with shared nested data
// structures. Scalar types are not cached as they are cheap to decode.
//
// The zero value of Unmarshaler is safe to use and will unmarshal data
// without caching. Use NewUnmarshaler() to create an Unmarshaler with
// caching enabled for better performance when loading full databases.
type Unmarshaler struct {
	cache  map[uint]DataType
	result DataType
}

var _ mmdbdata.CursorUnmarshaler = (*Unmarshaler)(nil)

// NewUnmarshaler creates a new Unmarshaler with caching enabled for converting
// MMDB data to mmdbtype values. The cache improves performance when loading
// databases with shared data structures by avoiding redundant decoding.
func NewUnmarshaler() *Unmarshaler {
	return &Unmarshaler{
		cache: map[uint]DataType{},
	}
}

// UnmarshalMaxMindDBCursor implements the mmdbdata.CursorUnmarshaler interface.
func (u *Unmarshaler) UnmarshalMaxMindDBCursor(
	cursor mmdbdata.Cursor,
) (mmdbdata.Cursor, error) {
	value, next, err := decodeDataTypeValue(cursor, u.cache)
	if err != nil {
		return mmdbdata.Cursor{}, err
	}

	u.result = value
	return next, nil
}

// Clear resets the unmarshaler state for reuse.
func (u *Unmarshaler) Clear() {
	u.result = nil
}

// Result returns the final unmarshaled value.
func (u *Unmarshaler) Result() DataType {
	return u.result
}
