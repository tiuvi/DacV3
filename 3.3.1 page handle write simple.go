package dacV3



func (sfDacV3 *DacV3) WritePage(hash [32]byte, data []byte, offset int64) error {

	sfIndexHandle, sfPageHandle, err := sfDacV3.newPageHandle(hash, data, offset, true)
	if err != nil {
		return err // Si no existe, devolverá tu errPagedNotFound
	}

	err = sfDacV3.writePageData(hash , sfIndexHandle, sfPageHandle, data, offset)
	if err != nil {
		return err
	}

	return nil
}

func (sfDacV3 *DacV3) WriteIfExistPage(hash [32]byte, data []byte, offset int64) error {
	// Llamamos a newPageHandle con create = false.
	// Él ya se encarga de verificar si existe y de devolvernos errPagedNotFound si no.
	sfIndexHandle, sfPageHandle, err := sfDacV3.newPageHandle(hash, data, offset, false)
	if err != nil {
		return err // Si no existe, devolverá tu errPagedNotFound
	}

	return sfDacV3.writePageData(hash , sfIndexHandle, sfPageHandle, data, offset)
}

// writePageData escribe datos en una pagina existente
// Decide entre WritePageDirect (datos nuevos mas alla de filelen) o WritePageWall (sobreescritura dentro de filelen)

func (sfDacV3 *DacV3) ReadPage(hash [32]byte, data []byte, offset int64) (n int, err error) {

	sfIndexHandle, sfPageHandle, err := sfDacV3.newPageHandle(hash, data, offset, true)
	if err != nil {
		return 0, err // Si no existe, devolverá tu errPagedNotFound
	}

	return sfDacV3.readPageData(sfIndexHandle, sfPageHandle, data, offset), nil
}

func (sfDacV3 *DacV3) Size(hash [32]byte) (size int64, err error) {

	sfIndexHandle, sfPageHandle, err := sfDacV3.newPageHandle(hash, nil, 0, true)
	if err != nil {
		return 0, err // Si no existe, devolverá tu errPagedNotFound
	}

	return sfIndexHandle.Buf.GetSubIndexSize(int(sfPageHandle.idSubIndex)) , nil
}

func (sfDacV3 *DacV3) ReadPageIfExist(hash [32]byte, data []byte, offset int64) (n int, err error) {
	// Llamamos a newPageHandle con create = false.
	// Él ya se encarga de verificar si existe y de devolvernos errPagedNotFound si no.
	sfIndexHandle, sfPageHandle, err := sfDacV3.newPageHandle(hash, data, offset, false)
	if err != nil {
		return 0, err // Si no existe, devolverá tu errPagedNotFound
	}

	return sfDacV3.readPageData(sfIndexHandle, sfPageHandle, data, offset), nil
}

func (sfDacV3 *DacV3) readPageData(sfIndex *indexHandle, sfPageHandle *pageHandle, data []byte, offset int64) (n int) {

	if offset < 0 {
		return 0
	}

	// 1. Obtenemos el tamaño real de los datos válidos en disco/memoria (filelen)
	fileLen := sfIndex.Buf.GetSubIndexSize(int(sfPageHandle.idSubIndex))

	// 2. Si el offset solicitado está más allá de lo que se ha escrito, no hay nada que leer
	if offset >= fileLen {
		return 0
	}

	// 3. Calculamos la longitud exacta a leer
	// Si nos piden leer más allá del tamaño real, cortamos la lectura hasta el fileLen
	readLen := int64(len(data))
	if offset+readLen > fileLen {
		readLen = fileLen - offset
	}

	// 4. Bloqueamos el buffer para evitar que otra gorutina lo modifique mientras leemos
	// NOTA: Si pb.mu es un sync.RWMutex, aquí deberías usar sfPageHandle.Buf.mu.RLock()
	sfPageHandle.Buf.mu.RLock()
	defer sfPageHandle.Buf.mu.RUnlock()

	// 6. Copiamos los datos desde la memoria de la página al slice del usuario
	copied := copy(data, sfPageHandle.Buf.buf[offset:offset+readLen])

	return copied
}

