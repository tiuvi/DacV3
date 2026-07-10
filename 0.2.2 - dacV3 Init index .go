package dacV3



func startHandleIndex(sfDacV3 *dacV3) {

	//Inicio donde se guardan los indices
	sfDacV3.indexLocation = NewPagedPool[Index]()
	
	//Donde se guardan los indices indexados a memoria
	sfDacV3.indexSearch = make(map[[32]byte]IndexSearch)

	sfDacV3.indexSearchPool = make(chan uint32, sfDacV3.opts.NChanAvaibleIndexSearch)

	sfDacV3.indexBufferDouble = MakeAlignedBlock(BufferAlignSize * 2)
	
	//Incicio de los buffers para los indices
	sfDacV3.indexBuffer = newBufferArena(sfDacV3.opts.NBuffersAvailableIndex, BufferAlignSize)

	sfDacV3.indexPools = make(map[uint32]chan uint32)
	//Inicio de los canales donde se van a guardar los indices
	for _, item := range sfDacV3.opts.SupportedSizes {
		sfDacV3.indexPools[item.Size] = make(chan uint32, item.IndexSizeChan)
	}

}