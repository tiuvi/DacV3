package dacV3

func startHandleData(sfDacV3 *DacV3) {

	sfDacV3.dataPools = make(map[int]*GlobalBufferPool)

	size := sfDacV3.indexMaster.blockMaxSize.pageSize

	sfDacV3.indexSearchDataPool = newBufferArena(sfDacV3.opts.NBuffersAvailableIndexSearch, size)

	for _, item := range sfDacV3.opts.SupportedSizes {
		sfDacV3.dataPools[int(item.Size)] = NewGlobalBufferPool(1000, item.NBuffersAvaibleData, int64(item.Size))
	}

	minSize := int(sfDacV3.indexMaster.blockMinSize.pageSize)
	maxSize := int(sfDacV3.indexMaster.blockMaxSize.pageSize)

	sfDacV3.writeDataPools = make(map[int]*bufferArena)

	// Iteramos incrementando de 4096 en 4096 bytes (pasos de 4KB)
	for size := minSize; size <= maxSize; size += 4096 {
		sfDacV3.writeDataPools[size] = newBufferArena(10, int64(size))
	}

	sfDacV3.pageLocation = NewPoolArray[Page](1000)

	sfDacV3.pages = NewConcurrentMap[uint32](64)

	initPages(sfDacV3)
}

func initPages(sfDacV3 *DacV3) {

	// Recorrer completamente indexLocation
	totalIndexes := sfDacV3.indexLocation.nextID

	for id := uint32(1); id <= totalIndexes; id++ {

		index := sfDacV3.indexLocation.Get(id)
		if index == nil {
			continue
		}

		// Solo procesar indices que tengan buffer cargado (offset != 0 significa que fue inicializado)
		if index.idLocationBuffer == 0 {
			continue
		}

		sfDacV3.InitAllPagesPerIndex(id, index)
	}
}
