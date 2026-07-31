package dacV3

import (
	"sync/atomic"
)

func (sfDacV3 *DacV3) SwapIndexDirection(sfIndexOld *indexHandle, idSubIndexOld uint8, sfIndexNew *indexHandle, idSubIndexNew uint8, newSize int64) {

	if TestCrashEnergy == CrashNewPageWhileWriteNewIndex {
		if TestCrashTarget.Load() == newSize || TestCrashTarget.CompareAndSwap(-1, newSize) {
			TestCrashEnergy = CrashWriteIndexCorrupt

			testCrashFuncIndex = func() bool {

				size := sfIndexNew.Buf.GetSubIndexSize(int(idSubIndexNew))

				if TestCrashTarget.Load() == size || TestCrashTarget.CompareAndSwap(-1, size) {

					sfIndexNew.Buf.unSetSubIndexHash(int(idSubIndexNew))
					sfIndexNew.Buf.SetSubIndexSequence(int(idSubIndexNew), 0)
					sfIndexNew.Buf.SetSubIndexSize(int(idSubIndexNew), 0)
					return true
				}
				return false
			}
		}
	}

	//ESte lock garantiza que hasta que se copia el buffer en update index no se libera el buffer
	sfIndexNew.mu.Lock()

	hash := sfIndexOld.Buf.GetSubIndexHash(int(idSubIndexOld))

	sequence := sfIndexOld.Buf.GetSubIndexSequence(int(idSubIndexOld))

	sfIndexNew.Buf.setSubIndexHash(hash, int(idSubIndexNew))

	sfIndexNew.Buf.SetSubIndexSequence(int(idSubIndexNew), sequence+1)

	sfIndexNew.Buf.SetSubIndexSize(int(idSubIndexNew), newSize)

	sfDacV3.updateIndex(sfIndexNew.Index, func() {
		sfIndexNew.mu.Unlock()
	})

	if TestCrashEnergy == CrashNewPageAfterNewIndexWrite {
		if TestCrashTarget.Load() == newSize || TestCrashTarget.CompareAndSwap(-1, newSize) {
			panic("SIMULANDO CORTE DE ENERGÍA 🔌💥 CrashNewPageAfterNewIndexWrite")
		}
	}

	if TestCrashEnergy == CrashNewPageWhileWriteOldIndex {

		if TestCrashTarget.Load() == newSize || TestCrashTarget.CompareAndSwap(-1, newSize) {

			TestCrashEnergy = CrashWriteIndexCorrupt

			testCrashFuncIndex = func() bool {

				size := sfIndexNew.Buf.GetSubIndexSize(int(idSubIndexNew))

				if TestCrashTarget.Load() == size || TestCrashTarget.CompareAndSwap(-1, size) {

					sfIndexOld.Buf.unSetSubIndexHash(int(idSubIndexOld))
					sfIndexOld.Buf.SetSubIndexSequence(int(idSubIndexOld), 0)
					sfIndexOld.Buf.SetSubIndexSize(int(idSubIndexOld), 0)
					return true
				}
				return false
			}
		}
	}

	//Esto se puede hacer ya en segundo plano

	sfIndexNew.mu.Lock()

	sfIndexOld.Buf.unSetSubIndexHash(int(idSubIndexOld))

	sfIndexOld.Buf.SetSubIndexSequence(int(idSubIndexOld), 0)

	sfIndexOld.Buf.SetSubIndexSize(int(idSubIndexOld), 0)

	sfIndexOld.Buf.UnSetIndexKept(int(idSubIndexOld))


	sfDacV3.updateIndex(sfIndexOld.Index, func() {
		sfIndexNew.mu.Unlock()
	})

}

func (sfDacV3 *DacV3) writePageDataSwapIndex(sfIndexOld *indexHandle, sfPageHandle *pageHandle, hash [32]byte, data []byte, dataLen int64, offset int64) error {

	dataEnd := offset + int64(len(data))

	sfPageHandle.mu.Lock()
	//Bloqueamos buffer antiguo para hacer cambio de buffer
	oldBuf := sfPageHandle.Buf
	oldBuf.mu.Lock()

	//Primeramente obtenemos un indice nuevo reservado, solo en memoria
	sfIndexNew, newIdIndex, newIdSubIndex, err := sfDacV3.UpdatePageInIndex(sfPageHandle.idSubIndex, uint32(dataEnd))
	if err != nil {
		oldBuf.mu.Unlock()
		sfPageHandle.mu.Unlock()
		return err
	}

	oldIndexSubIndex := sfPageHandle.idSubIndex
	// ¡CRÍTICO! El sub-índice DEBE asignarse ANTES del atomic.Store.
	sfPageHandle.idSubIndex = newIdSubIndex

	//Asignamos el nuevo indice a la pagina
	atomic.StoreUint32(&sfPageHandle.idIndex, newIdIndex)

	sizePagination := sfIndexNew.Buf.GetSizePagination()

	arena := sfDacV3.dataPools[int(sizePagination)]

	newIdBuffer, newBuf := arena.New()

	//Copiamos antiguo buffer al nuevo
	newBuf.CopyAt(0, oldBuf.buf)

	//Escribimos datos en el NUEVO buffer
	newBuf.CopyAt(offset, data)

	sfPageHandle.Buf = newBuf

	oldIdBuffer := atomic.LoadUint32(&sfPageHandle.idBuffer)

	atomic.StoreUint32(&sfPageHandle.idBuffer, newIdBuffer)

	oldBuf.mu.Unlock()

	// Liberamos el buffer antiguo
	oldArena := sfDacV3.dataPools[int(sfIndexOld.Buf.GetSizePagination())]

	oldArena.Free(oldIdBuffer)

	// Estoy hay que hacerlo con el nuevo indice
	pageStartOffset := sfDacV3.getOffsetPageStart(sfIndexNew.Index, sfPageHandle.Page)

	if TestCrashEnergy == CrashNewPageBeforeWrite {

		if TestCrashTarget.Load() == dataEnd || TestCrashTarget.CompareAndSwap(-1, dataEnd) {
			panic("SIMULANDO CORTE DE ENERGÍA 🔌💥 CrashNewPageBeforeWrite")
		}
	}

	if TestCrashEnergy == CrashNewPageWhileWriteData {
		if TestCrashTarget.Load() == dataEnd || TestCrashTarget.CompareAndSwap(-1, dataEnd) {
			TestCrashEnergy = CrashWriteUnsafeCorrupt
			println("SIMULANDO CORTE DE ENERGÍA 🔌💥 CrashNewPageWhileWriteData")
		}
	}

	if TestCrashEnergy == CrashNewPageWhileWriteWalData {
		if TestCrashTarget.Load() == dataEnd || TestCrashTarget.CompareAndSwap(-1, dataEnd) {
			TestCrashEnergy = CrashWriteWallCorrupt
			println("SIMULANDO CORTE DE ENERGÍA 🔌💥 CrashNewPageWhileWriteWalData")
		}
	}

	if TestCrashEnergy == CrashNewPageWhileWriteWalIndex {
		if TestCrashTarget.Load() == dataEnd || TestCrashTarget.CompareAndSwap(-1, dataEnd) {
			TestCrashEnergy = CrashWriteWallIndexCorrupt
			println("SIMULANDO CORTE DE ENERGÍA 🔌💥 CrashNewPageWhileWriteWalIndex")
		}
	}

	err = sfDacV3.WritePageDirect(hash, offset, dataLen, newBuf.buf, pageStartOffset, func() {

		sfPageHandle.mu.Unlock()
	})
	if err != nil {
		return err
	}

	if TestCrashEnergy == CrashWriteUnsafeCorrupt {
		if TestCrashTarget.Load() == dataEnd || TestCrashTarget.CompareAndSwap(-1, dataEnd) {
			// 1. Esperamos a que el worker haga la escritura a medias y nos avise
			<-TestCrashEnergiChan

			// 2. Explotamos AQUÍ, en el hilo principal.
			// Como estamos en el hilo principal, el test SÍ podrá atrapar este panic con defer recover()
			panic("SIMULANDO CORTE DE ENERGÍA 🔌💥 CrashWriteUnsafeCorrupt (Controlado)")
		}
	}

	if TestCrashEnergy == CrashWriteWallCorrupt || TestCrashEnergy == CrashWriteWallIndexCorrupt {
		if TestCrashTarget.Load() == dataEnd || TestCrashTarget.CompareAndSwap(-1, dataEnd) {
			// Explotamos AQUÍ, en el hilo principal.
			// Como estamos en el hilo principal, el test SÍ podrá atrapar este panic con defer recover()
			panic("SIMULANDO CORTE DE ENERGÍA 🔌💥 CrashWall (Controlado)")
		}
	}

	if TestCrashEnergy == CrashNewPageAfterWrite {
		if TestCrashTarget.Load() == dataEnd || TestCrashTarget.CompareAndSwap(-1, dataEnd) {
			println("CrashNewPageAfterWrite DAtos escritos: ", gettLastLine(string(newBuf.buf)))
			panic("SIMULANDO CORTE DE ENERGÍA 🔌💥 CrashNewPageAfterWrite")
		}
	}

	sfDacV3.SwapIndexDirection(sfIndexOld, oldIndexSubIndex, sfIndexNew, newIdSubIndex, dataEnd)

	return nil
}
