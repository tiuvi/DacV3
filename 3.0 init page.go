package dacV3

// Inicia todas las paginas de un indice, añadie su id y posicion al mapa de datos pero no el buffer
// Si el indice es indexSearch no se inicia
func (sfDacV3 *dacV3) InitAllPagesPerIndex(idIndex uint32, index *Index) {

	buf := sfDacV3.indexBuffer.getBufferArena(index.idLocationBuffer)
	if buf == nil {
		return
	}

	bufIndex := indexBuffer(buf)

	// Si el indice es indexSearch no se inicia
	var emptyHash [32]byte
	if bufIndex.GetHashSearch() != emptyHash {
		return
	}

	var needsUpdate bool

	// Comprobar lista de activados con todos los subindices de paginas
	for i := 0; i < MaxSubIndexPerIndex; i++ {

		if !bufIndex.IsIndexKept(i) {
			continue
		}

		// si en la lista se activo pero el subindice esta vacio borrarlo de la lista de activados
		size := bufIndex.GetSubIndexSize(i)
		if size == 0 {

			bufIndex.unSetSubIndex(i)
			needsUpdate = true
			continue
		}

		hash := bufIndex.GetSubIndexHash(i)

		seq := bufIndex.GetSubIndexSequence(i)

		idPage, exists := sfDacV3.pages.Get(hash)
		if exists {

			existingPage := sfDacV3.pageLocation.Get(uint32(idPage))

			existingIndex := sfDacV3.indexLocation.Get(existingPage.idIndex)

			existingBuf := sfDacV3.indexBuffer.getBufferArena(existingIndex.idLocationBuffer)
			if existingBuf != nil {

				existingBufIndex := indexBuffer(existingBuf)

				existingSeq := existingBufIndex.GetSubIndexSequence(int(existingPage.idSubIndex))

				if seq > existingSeq {

					existingPage.mu.Lock()
					existingPage.idIndex = idIndex
					existingPage.idSubIndex = uint8(i)
					existingPage.mu.Unlock()

				}

			}

		} else {

			newIdPage, newPage := sfDacV3.pageLocation.New()

			newPage.idIndex = idIndex

			newPage.idSubIndex = uint8(i)

			sfDacV3.pages.StoreOrGet(hash, func() uint32 {
				return newIdPage
			})

		}
	}

	if needsUpdate {

		sfDacV3.updateIndex(index)

	}
}
