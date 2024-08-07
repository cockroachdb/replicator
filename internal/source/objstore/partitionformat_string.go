// Code generated by "stringer -type=PartitionFormat"; DO NOT EDIT.

package objstore

import "strconv"

func _() {
	// An "invalid array index" compiler error signifies that the constant values have changed.
	// Re-run the stringer command to generate them again.
	var x [1]struct{}
	_ = x[Daily-0]
	_ = x[Flat-1]
	_ = x[Hourly-2]
}

const _PartitionFormat_name = "DailyFlatHourly"

var _PartitionFormat_index = [...]uint8{0, 5, 9, 15}

func (i PartitionFormat) String() string {
	if i < 0 || i >= PartitionFormat(len(_PartitionFormat_index)-1) {
		return "PartitionFormat(" + strconv.FormatInt(int64(i), 10) + ")"
	}
	return _PartitionFormat_name[_PartitionFormat_index[i]:_PartitionFormat_index[i+1]]
}
