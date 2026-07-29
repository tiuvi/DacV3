package dacV3

func (sfDacV3 *DacV3) writePageWalData(sfIndex *indexHandle,
	dataEnd int64,
	fileLen int64,
	sfPageHandle *pageHandle,
	hash [32]byte,
	offset int64,
	dataLen int64,
	alignedDataView []byte,
	absoluteAlignedOffset int64) (err error) {

	if TestCrashEnergy == CrashPageWalWhileWriteData {
		TestCrashEnergy = CrashWriteUnsafeCorrupt
		println("SIMULANDO CORTE DE ENERGÍA 🔌💥 CrashPageWalWhileWriteData")
	}

	if TestCrashEnergy == CrashPageWalWhileWriteWalData {
		TestCrashEnergy = CrashWriteWallCorrupt
		println("SIMULANDO CORTE DE ENERGÍA 🔌💥 CrashPageWalWhileWriteWalData")
	}

	if TestCrashEnergy == CrashPageWalWhileWriteWalIndex {
		TestCrashEnergy = CrashWriteWallIndexCorrupt
		println("SIMULANDO CORTE DE ENERGÍA 🔌💥 CrashPageWalWhileWriteWalIndex")
	}

	// ESCRITURA WALL (Update/Sobrescritura)
	sfPageHandle.mu.Lock()

	// Enviamos el slice alineado a 4K y el offset absoluto alineado a 4K
	err = sfDacV3.WritePageWall(hash, offset, dataLen, alignedDataView, absoluteAlignedOffset, func() {
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

	if TestCrashEnergy == CrashPageWalAfterWrite {
		// Explotamos AQUÍ, en el hilo principal.
		// Como estamos en el hilo principal, el test SÍ podrá atrapar este panic con defer recover()
		panic("SIMULANDO CORTE DE ENERGÍA 🔌💥 CrashPageWalAfterWrite (Controlado)")
	}

	if TestCrashEnergy == CrashPageWalWhileWriteIndex {
		TestCrashEnergy = CrashWriteIndexCorrupt
	}

	// 6. Actualizar el filelen si los datos escritos extienden el tamaño
	if dataEnd > fileLen {

		sfIndex.mu.Lock()

		sfIndex.SetSubIndexSize(int(sfPageHandle.idSubIndex), dataEnd)

		sfDacV3.updateIndex(sfIndex.Index)

		sfIndex.mu.Unlock()
	}

	return
}
