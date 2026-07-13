package dacV3

func startHandleData(sfDacV3 *dacV3) {

	sfDacV3.dataPools = make(map[int]*bufferArena)

	size := sfDacV3.indexMaster.blockMaxSize.pageSize

	sfDacV3.indexSearchDataPool = newBufferArena(sfDacV3.opts.NBuffersAvailableIndexSearch, size)

	for _, item := range sfDacV3.opts.SupportedSizes {
		sfDacV3.dataPools[int(item.Size)] = newBufferArena(item.nBuffersAvaibleData, int64(item.Size))
	}

	sfDacV3.pageLocation = NewPoolArray[Page]()

	sfDacV3.pages = make(map[[32]byte]int)

	initPages(sfDacV3)
}

func initPages(sfDacV3 *dacV3) {

	// Recorrer completamente indexLocation
	totalIndexes := sfDacV3.indexLocation.nextID.Load()

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
