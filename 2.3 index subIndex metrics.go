package dacV3

import (
	"encoding/binary"
)

func (b indexBufferMetric) SetSubIndexLastAccess(id int, lastAccess int64) {

	if id > maxSubIndexPerIndex {
		panic(errSubIndexOverFlow)
	}

	offsetIndex := id * sizeSubIndexMetric

	// LastAccess ocupa desde el byte 40 al 48 relativos a este subíndice
	binary.BigEndian.PutUint64(b[offsetIndex+subIndex_LastAccess_Init:offsetIndex+subIndex_LastAccess_End], uint64(lastAccess))
}

func (b indexBufferMetric) GetSubIndexLastAccess(id int) int64 {
	if id > maxSubIndexPerIndex {
		panic(errSubIndexOverFlow)
	}

	offsetIndex := id * sizeSubIndexMetric

	val := binary.BigEndian.Uint64(b[offsetIndex+subIndex_LastAccess_Init : offsetIndex+subIndex_LastAccess_End])
	return int64(val)
}

func (b indexBufferMetric) SetSubIndexLastUpdate(id int, lastUpdate int64) {

	if id > maxSubIndexPerIndex {
		panic(errSubIndexOverFlow)
	}

	offsetIndex := id * sizeSubIndexMetric

	// LastUpdate ocupa desde el byte 48 al 56 relativos a este subíndice
	binary.BigEndian.PutUint64(b[offsetIndex+subIndex_LastUpdate_Init:offsetIndex+subIndex_LastUpdate_End], uint64(lastUpdate))
}

func (b indexBufferMetric) GetSubIndexLastUpdate(id int) int64 {

	if id > maxSubIndexPerIndex {
		panic(errSubIndexOverFlow)
	}

	offsetIndex := id * sizeSubIndexMetric

	val := binary.BigEndian.Uint64(b[offsetIndex+subIndex_LastUpdate_Init : offsetIndex+subIndex_LastUpdate_End])
	return int64(val)
}
