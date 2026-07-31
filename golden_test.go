package mmdbwriter

import (
	"bytes"
	"encoding/hex"
	"net/netip"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/maxmind/mmdbwriter/v2/mmdbtype"
)

// This fixture was captured from the pre-value-store v2 implementation. It
// pins node order, data emission order, greedy pointers, sorted map keys, and
// metadata encoding across the storage redesign.
const enterpriseGoldenHex = "00000100000011000002000000280000030000002800000400000028000005000000280000060000002800000700000010000028000000080000090000003800000a0000003800000b0000003800000c0000003800000d0000003800000e000000380000380000000f0000f10000003800003800000028000028000000120000130000002800001400000028000028000000150000160000002800001700000028000018000000280000190000002800001a0000002800001b0000002800001c0000002800001d0000002800001e0000002800001f0000002800002800000020000021000000280000220000002800002300000028000024000000280000250000002800002600000028000028000000270000f10000002800000000000000000000000000000000e44463697479e24a67656f6e616d655f6964c328571f456e616d6573e242656e464c6f6e646f6e426672474c6f6e6472657347636f756e747279e24869736f5f636f64654247422016e1201d4e556e69746564204b696e67646f6d486c6f636174696f6ee34f61636375726163795f726164697573a10a486c61746974756465684049c0f27bb2fec5496c6f6e67697475646568bfc05bc01a36e2eb46747261697473e15269735f616e6f6e796d6f75735f70726f78790007e4200120062032203a205b2064209ce4586175746f6e6f6d6f75735f73797374656d5f6e756d626572c2fc0046646f6d61696e4c6578616d706c652e74657374436973704b4578616d706c652049535049757365725f7479706548627573696e657373abcdef4d61784d696e642e636f6de95b62696e6172795f666f726d61745f6d616a6f725f76657273696f6ea1025b62696e6172795f666f726d61745f6d696e6f725f76657273696f6ea04b6275696c645f65706f63680402075bcd154d64617461626173655f7479706551456e74657270726973652d476f6c64656e4b6465736372697074696f6ee142656e4f476f6c64656e2064617461626173654a69705f76657273696f6ea104496c616e6775616765730204207b4266724a6e6f64655f636f756e74c1284b7265636f72645f73697a65a11c"

func TestEnterpriseShapedOutputMatchesPreStoreGolden(t *testing.T) {
	tree, err := New(Options{
		BuildEpoch:              123456789,
		DatabaseType:            "Enterprise-Golden",
		Description:             map[string]string{"en": "Golden database"},
		IPVersion:               4,
		IncludeReservedNetworks: true,
		Languages:               []string{"en", "fr"},
		RecordSize:              28,
	})
	require.NoError(t, err)
	names := mmdbtype.Map{"en": mmdbtype.String("London"), "fr": mmdbtype.String("Londres")}
	city := mmdbtype.Map{
		"city": mmdbtype.Map{"geoname_id": mmdbtype.Uint32(2643743), "names": names},
		"country": mmdbtype.Map{
			"iso_code": mmdbtype.String("GB"),
			"names":    mmdbtype.Map{"en": mmdbtype.String("United Kingdom")},
		},
		"location": mmdbtype.Map{
			"accuracy_radius": mmdbtype.Uint16(10),
			"latitude":        mmdbtype.Float64(51.5074),
			"longitude":       mmdbtype.Float64(-0.1278),
		},
		"traits": mmdbtype.Map{"is_anonymous_proxy": mmdbtype.Bool(false)},
	}
	enterprise := city.Copy().(mmdbtype.Map)
	enterprise["traits"] = mmdbtype.Map{
		"autonomous_system_number": mmdbtype.Uint32(64512),
		"domain":                   mmdbtype.String("example.test"),
		"isp":                      mmdbtype.String("Example ISP"),
		"user_type":                mmdbtype.String("business"),
	}
	for _, item := range []struct {
		prefix string
		value  mmdbtype.DataType
	}{
		{"1.0.0.0/8", city},
		{"1.2.0.0/16", enterprise},
		{"2.0.0.0/8", city.Copy()},
		{"200.1.2.0/24", enterprise.Copy()},
	} {
		require.NoError(t, tree.Insert(netip.MustParsePrefix(item.prefix), item.value))
	}

	var output bytes.Buffer
	_, err = tree.WriteTo(&output)
	require.NoError(t, err)
	require.Equal(t, enterpriseGoldenHex, hex.EncodeToString(output.Bytes()))
}
