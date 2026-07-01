package collecttop

import "bsc_stats/common"

// normTxType maps a tx type hex string to a canonical 0x00..0x04 form.
// Missing/empty type is treated as Legacy (0x0).
func normTxType(t string) int {
	if t == "" {
		return 0
	}
	v, err := common.ParseHexInt(t)
	if err != nil {
		return 0
	}
	return int(v)
}
