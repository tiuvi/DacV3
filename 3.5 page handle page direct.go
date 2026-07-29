package dacV3



func (sfDacV3 *DacV3) writePageDirectData(sfIndex *indexHandle,
	dataEnd int64,
	fileLen int64,
	sfPageHandle *pageHandle,
	hash [32]byte,
	offset int64,
	dataLen int64,
	alignedDataView []byte,
	absoluteAlignedOffset int64) (err error) {

	if TestCrashEnergy == CrashPageDirectWhileWriteData {
		TestCrashEnergy = CrashWriteUnsafeCorrupt
		println("SIMULANDO CORTE DE ENERGÍA 🔌💥 CrashPageDirectWhileWriteData")
	}

	if TestCrashEnergy == CrashPageDirectWhileWriteWalData {
		TestCrashEnergy = CrashWriteWallCorrupt
		println("SIMULANDO CORTE DE ENERGÍA 🔌💥 CrashPageDirectWhileWriteWalData")
	}

	if TestCrashEnergy == CrashPageDirectWhileWriteWalIndex {
		TestCrashEnergy = CrashWriteWallIndexCorrupt
		println("SIMULANDO CORTE DE ENERGÍA 🔌💥 CrashPageDirectWhileWriteWalIndex")
	}

	// ESCRITURA DIRECTA (Append)
	sfPageHandle.mu.Lock()

	// Enviamos el slice alineado a 4K y el offset absoluto alineado a 4K
	err = sfDacV3.WritePageDirect(hash, offset, dataLen, alignedDataView, absoluteAlignedOffset, func() {
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
		panic("SIMULANDO CORTE DE ENERGÍA 🔌💥 CrashNewPageWhileWriteData (Controlado)")
	}

	if TestCrashEnergy == CrashWriteWallCorrupt || TestCrashEnergy == CrashWriteWallIndexCorrupt {

		// Explotamos AQUÍ, en el hilo principal.
		// Como estamos en el hilo principal, el test SÍ podrá atrapar este panic con defer recover()
		panic("SIMULANDO CORTE DE ENERGÍA 🔌💥 CrashWall (Controlado)")
	}

	if TestCrashEnergy == CrashPageDirectAfterWrite {

		// Explotamos AQUÍ, en el hilo principal.
		// Como estamos en el hilo principal, el test SÍ podrá atrapar este panic con defer recover()
		panic("SIMULANDO CORTE DE ENERGÍA 🔌💥 CrashPageDirectAfterWrite (Controlado)")
	}

	if TestCrashEnergy == CrashPageDirectWhileWriteIndex {
		TestCrashEnergy = CrashWriteIndexCorrupt
	}

	if dataEnd > fileLen {

		sfIndex.mu.Lock()

		sfIndex.SetSubIndexSize(int(sfPageHandle.idSubIndex), dataEnd)

		sfDacV3.updateIndex(sfIndex.Index)

		sfIndex.mu.Unlock()
	}

	return
}
