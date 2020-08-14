module github.com/maxmind/mmdbwriter

go 1.14

require (
	// Note: we are currently using the greg/mmdbtypes branch of the
	// maxminddb reader. Upstream is hesitant about merging this as
	// it would make the reader depend on mmdbwriter for the mmdbtype
	// package. This perhaps can be revisited once the mmdbwriter API
	// is finalized. Another alternative would be to have a vendored
	// fork of maxminddb in an internal/ subdirectory.
	github.com/oschwald/maxminddb-golang v1.7.1-0.20200814161932-f4d6bb67024d
	github.com/pkg/errors v0.9.1
	github.com/stretchr/testify v1.6.1
)
