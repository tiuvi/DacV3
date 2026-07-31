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

		case <-sfDacV3.ctx.Done():
			return nil, 0, 0, sfDacV3.ctx.Err()

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

				select {

				case sfDacV3.needIndexChan <- newIndexRequest{
					sizePagination: int64(size),
					isSearch:       false,
				}:
				case <-sfDacV3.ctx.Done():
					return nil, 0, 0, sfDacV3.ctx.Err()
				default:
					// Si la cola está llena, ignoramos el aviso para no congelar la escritura
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

		newIdSubIndex, found := sfIndexHandle.Buf.GetFirstEmptyIndex()
		if !found {
			return false, 0, 0
		}

		sfIndexHandle.Buf.SetIndexKept(newIdSubIndex)

		sfIndexHandle.Buf.SetSubIndexSequence(newIdSubIndex, 0)

		sfIndexHandle.Buf.SetSubIndexSize(newIdSubIndex , 0)
		
		sfIndexHandle.Buf.unSetSubIndexHash(newIdSubIndex)

		return true, idIndex, uint8(newIdSubIndex)
	})
}

func (sfDacV3 *DacV3) UpdatePageInIndex(idSubIndexCurrent uint8, requiredSpace uint32) (sfIndexHandle *indexHandle, newIdIndex uint32, newIdSubIndex uint8, err error) {

	return sfDacV3.handlePageInIndex(requiredSpace, func(sfIndexHandle *indexHandle, idIndex uint32) (bool, uint32, uint8) {

		sfIndexHandle.mu.Lock()
		defer sfIndexHandle.mu.Unlock()

		newIdSubIndex, found := sfIndexHandle.Buf.GetFirstEmptyIndex()
		if !found {
			return false, 0, 0
		}

		sfIndexHandle.Buf.SetIndexKept(newIdSubIndex)

		return true, idIndex, uint8(newIdSubIndex)
	})
}


