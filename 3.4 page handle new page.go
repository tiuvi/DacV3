package dacV3

import (
	"sync/atomic"
)

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

	sizePagination := sfIndexNew.GetSizePagination()

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
	oldArena := sfDacV3.dataPools[int(sfIndexOld.GetSizePagination())]

	oldArena.Free(oldIdBuffer)

	// Estoy hay que hacerlo con el nuevo indice
	pageStartOffset := sfDacV3.getOffsetPageStart(sfIndexNew.Index, sfPageHandle.Page)

	if TestCrashEnergy == CrashNewPageBeforeWrite {
		panic("SIMULANDO CORTE DE ENERGÍA 🔌💥 CrashNewPageBeforeWrite")
	}

	if TestCrashEnergy == CrashNewPageWhileWriteData {
		TestCrashEnergy = CrashWriteUnsafeCorrupt
		println("SIMULANDO CORTE DE ENERGÍA 🔌💥 CrashNewPageWhileWriteData")
	}

	if TestCrashEnergy == CrashNewPageWhileWriteWalData {
		TestCrashEnergy = CrashWriteWallCorrupt
		println("SIMULANDO CORTE DE ENERGÍA 🔌💥 CrashNewPageWhileWriteWalData")
	}

	if TestCrashEnergy == CrashNewPageWhileWriteWalIndex {
		TestCrashEnergy = CrashWriteWallIndexCorrupt
		println("SIMULANDO CORTE DE ENERGÍA 🔌💥 CrashNewPageWhileWriteWalIndex")
	}

	err = sfDacV3.WritePageDirect(hash, offset, dataLen, newBuf.buf, pageStartOffset, func() {

		sfPageHandle.mu.Unlock()
	})
	if err != nil {
		return err
	}

	if TestCrashEnergy == CrashWriteUnsafeCorrupt {
		// 1. Esperamos a que el worker haga la escritura a medias y nos avise
		<-TestCrashEnergiChan

		// 2. Explotamos AQUÍ, en el hilo principal.
		// Como estamos en el hilo principal, el test SÍ podrá atrapar este panic con defer recover()
		panic("SIMULANDO CORTE DE ENERGÍA 🔌💥 CrashWriteUnsafeCorrupt (Controlado)")
	}

	if TestCrashEnergy == CrashWriteWallCorrupt || TestCrashEnergy == CrashWriteWallIndexCorrupt {

		// Explotamos AQUÍ, en el hilo principal.
		// Como estamos en el hilo principal, el test SÍ podrá atrapar este panic con defer recover()
		panic("SIMULANDO CORTE DE ENERGÍA 🔌💥 CrashWall (Controlado)")
	}

	if TestCrashEnergy == CrashNewPageAfterWrite {
		panic("SIMULANDO CORTE DE ENERGÍA 🔌💥 CrashNewPageAfterWrite")
	}

	sfDacV3.SwapIndexDirection(sfIndexOld, oldIndexSubIndex, sfIndexNew, newIdSubIndex, dataEnd)

	println("SE termina de ejecutar la ampliacion")
	return nil
}
