package dacV3


func startHandleData(sfDacV3 *dacV3) {

	sfDacV3.dataPools = make(map[int]*bufferArena)

	size := sfDacV3.indexMaster.blockMaxSize.pageSize
	
	sfDacV3.indexSearchDataPool = newBufferArena(sfDacV3.opts.NBuffersAvailableIndexSearch, size)

	for _, item := range sfDacV3.opts.SupportedSizes {
		sfDacV3.dataPools[int(item.Size)] = newBufferArena(item.nBuffersAvaibleData, int64(item.Size))
	}

}