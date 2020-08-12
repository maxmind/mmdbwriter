package mmdbwriter

// XXX separate package?

// Remove any records for the network being inserted.
func Remove(value DataType) DataType {
	return nil
}

// ReplaceWith generates an inserter function that replaces the existing
// value with the new value.
func ReplaceWith(value DataType) func(DataType) DataType {
	return func(_ DataType) DataType {
		return value
	}
}
