// Package ctrlsize encodes the size field of a MaxMind DB control byte.
package ctrlsize

const (
	oneByte   = 29
	twoBytes  = oneByte + 1<<8
	threeByte = twoBytes + 1<<16

	// Max is the largest size that a control byte can encode.
	Max = threeByte + 1<<24 - 1
)

// Split returns how size is encoded: the 5 size bits of the control byte, the
// value of the size bytes that follow, and the number of those bytes. size
// must be at most Max.
func Split(size int) (bits byte, extra, extraBytes int) {
	switch {
	case size < oneByte:
		return byte(size), 0, 0 //nolint:gosec // this branch bounds size below 29
	case size < twoBytes:
		return 29, size - oneByte, 1
	case size < threeByte:
		return 30, size - twoBytes, 2
	default:
		return 31, size - threeByte, 3
	}
}
