package dacV3

import (
	"sync/atomic"
)


/*
const SsdNIopsMili = 2000

const totalWalIndexBuffer = (SsdNIopsMili * BufferAlignSize)

const totalWalDataBuffer = (SsdNIopsMili * 65536)

const totalIndexSumData = (totalWalIndexBuffer + totalWalDataBuffer) * 3

const totalInMb = totalIndexSumData / int64(Megabyte)
*/

type DacV3Options struct {
	DacRoute               string
	Truncate bool
	SizeIndexMaster        int
	MaxReserveSize         int64
	SsdNIopsMili           uint32
	NBuffersAvailableIndex uint32

	queueChanMultiplier uint32
	minPercentajeTotalSlotsCreate int

	NChanAvaibleIndexSearch      uint32
	NBuffersAvailableIndexSearch uint32

	NBuffersAvailableIndexSearchData uint32
	SupportedSizes                   []SizeConfig
	NWorkers                         int
	QueueSize                        int
}

type WalZeroCopy struct {
	id       uint64
	offset   uint64
	size     uint64
	sequence atomic.Uint64
}

type WalAppend struct {
	id       uint64
	offset   uint64
	size     uint64
	sequence atomic.Uint64
}

const totalClusterPages = 65536 / 32
const totalBytesPerClusterPage = 65536 * totalClusterPages
const totalBytesPerClusterPageMb = totalBytesPerClusterPage / int64(Megabyte)

const totalIndexbytes = Terabyte / (MaxSubIndexPerIndex * 4096)
const totalIndexMb = (totalIndexbytes * BufferAlignSize) / Gigabyte
