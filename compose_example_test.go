package mmdbwriter_test

import (
	"fmt"
	"net/netip"

	"github.com/maxmind/mmdbwriter/v2"
	"github.com/maxmind/mmdbwriter/v2/mmdbtype"
)

func ExampleCompose() {
	base := mmdbwriter.NewSortingSource(nil)
	if err := base.Insert(netip.MustParsePrefix("1.0.0.0/8"), mmdbtype.String("base")); err != nil {
		panic(err)
	}
	overlay := mmdbwriter.SourceFunc(func(yield func(mmdbwriter.NetworkValue, error) bool) {
		yield(
			mmdbwriter.NetworkValue{
				Prefix: netip.MustParsePrefix("1.2.0.0/16"),
				Value:  mmdbtype.String("overlay"),
			},
			nil,
		)
	})
	// A nil merge selects the last layer with a value at each network.
	tree, err := mmdbwriter.Compose(
		mmdbwriter.Options{},
		[]mmdbwriter.NetworkSource{base, overlay},
		nil,
	)
	if err != nil {
		panic(err)
	}
	_, value := tree.Get(netip.MustParseAddr("1.2.3.4"))
	fmt.Println(value)
	// Output: overlay
}
