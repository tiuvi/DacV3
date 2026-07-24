package dacV3

func (sfDacV3 *DacV3) handlePageInIndex(requiredSpace uint32, handle func(sfIndexHandle *indexHandle, idIndex uint32) (success bool, newIdIndex uint32, newIdSubIndex uint8)) (sfIndexHandle *indexHandle, newIdIndex uint32, newIdSubIndex uint8, err error) {

	size := sfDacV3.GetSizeForIndex(requiredSpace)
	if size == 0 {
		return nil, 0, 0, ErrNoSpaceAllocated
	}

	pool := sfDacV3.indexPools[size]
	success := false

	for intento := 0; intento < 100; intento++ {

		select {

		case idIndex := <-pool:

			if idIndex.AvailableSlots == 0 {

				continue

			} else {

				idIndex.AvailableSlots = idIndex.AvailableSlots - 1
				pool <- idIndex

			}

			sfDacV3.indexAvailableSlots[size].Add(-1)

			//Cada vez que globalmente nos gastemos un indice mandamos un aviso
			if sfDacV3.indexAvailableSlots[size].Load()%MaxSubIndexPerIndex == 0 {

				println("slots libres: ", sfDacV3.indexAvailableSlots[size].Load())

				sfDacV3.needIndexChan <- newIndexRequest{
					sizePagination: int64(size),
					isSearch:       false,
				}

			}

			sfIndexHandle, err = sfDacV3.newIndexHandle(idIndex.IDIndex)
			if err != nil {
				// Aquí sí usamos fmt.Errorf para añadir contexto dinámico (el idIndex)
				return nil, 0, 0, err
			}

			success, newIdIndex, newIdSubIndex = handle(sfIndexHandle, idIndex.IDIndex)

		default:

			println("Error de configuracion de bases de datos, generacion de indices demasiado lento.")
			err = sfDacV3.newIndexs(1, int64(size), false)
			if err != nil {
				return nil, 0, 0, err
			}

		}

		if success {
			break
		}

	}

	if success {
		// Lo logramos para este tamaño, salimos del bucle
		return sfIndexHandle, newIdIndex, newIdSubIndex, nil
	}

	// RETORNAMOS LA VARIABLE GLOBAL DE ERROR
	return nil, 0, 0, ErrNoSpaceAllocated

}

func (sfDacV3 *DacV3) CreatePageInIndex(hash [32]byte, requiredSpace uint32) (sfIndexHandle *indexHandle, newIdIndex uint32, newIdSubIndex uint8, err error) {

	return sfDacV3.handlePageInIndex(requiredSpace, func(sfIndexHandle *indexHandle, idIndex uint32) (bool, uint32, uint8) {

		sfIndexHandle.mu.Lock()
		defer sfIndexHandle.mu.Unlock()

		newIdSubIndex, found := sfIndexHandle.GetFirstEmptyIndex()
		if !found {
			return false, 0, 0
		}

		sfIndexHandle.SetIndexKept(newIdSubIndex)

		sfIndexHandle.SetSubIndexSequence(newIdSubIndex, 0)

		sfIndexHandle.setSubIndexHash(hash, newIdSubIndex)

		return true, idIndex, uint8(newIdSubIndex)
	})
}

func (sfDacV3 *DacV3) UpdatePageInIndex(idSubIndexCurrent uint8, requiredSpace uint32) (sfIndexHandle *indexHandle, newIdIndex uint32, newIdSubIndex uint8, err error) {

	return sfDacV3.handlePageInIndex(requiredSpace, func(sfIndexHandle *indexHandle, idIndex uint32) (bool, uint32, uint8) {

		sfIndexHandle.mu.Lock()
		defer sfIndexHandle.mu.Unlock()

		newIdSubIndex, found := sfIndexHandle.GetFirstEmptyIndex()
		if !found {
			return false, 0, 0
		}

		return true, idIndex, uint8(newIdSubIndex)
	})
}

func (sfDacV3 *DacV3) SwapIndexDirection(sfIndexOld *indexHandle, idSubIndexOld uint8, sfIndexNew *indexHandle, idSubIndexNew uint8, newSize int64) {

	sfIndexOld.mu.Lock()
	hash := sfIndexOld.GetSubIndexHash(int(idSubIndexOld))
	sequence := sfIndexOld.GetSubIndexSequence(int(idSubIndexOld))
	sfIndexOld.mu.Unlock()

	sfIndexNew.mu.Lock()

	sfIndexNew.setSubIndexHash(hash, int(idSubIndexNew))

	sfIndexNew.SetSubIndexSequence(int(idSubIndexNew), sequence+1)

	sfIndexNew.SetSubIndexSize(int(idSubIndexNew), newSize)

	sfDacV3.updateIndex(sfIndexNew.Index)

	sfIndexNew.mu.Unlock()

	if TestCrashEnergy == CrashDuringIndexSwap {
		panic("SIMULANDO CORTE DE ENERGÍA 🔌💥 CrashDuringIndexSwap")
	}

	//Esto se puede hacer ya en segundo plano
	sfIndexOld.mu.Lock()

	sfIndexOld.unSetSubIndexHash(int(idSubIndexOld))

	sfIndexOld.SetSubIndexSequence(int(idSubIndexOld), 0)

	sfIndexOld.SetSubIndexSize(int(idSubIndexOld), 0)

	sfIndexOld.UnSetIndexKept(int(idSubIndexOld))

	sfDacV3.updateIndex(sfIndexOld.Index)

	sfIndexOld.mu.Unlock()

}
