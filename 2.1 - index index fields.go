package dacV3

import (
	"encoding/binary"
	"hash/crc32"
)
 
func (globBuf *indexBuffer) calCheckSum() uint32 {

	// Se calcula el checksum desde el final del checksum hasta el límite de la página
	return crc32.Checksum(globBuf.buf[field_IndexCheckSumEnd:BufferAlignSize], castagnoliTable)
}

func (globBuf *indexBuffer) CalCheckSum() uint32 {
	globBuf.mu.RLock()
	defer globBuf.mu.RUnlock()
	return globBuf.calCheckSum()
}

// SetCheckSum calcula el checksum de data y lo escribe en la sección de checksum de index
func (globBuf *indexBuffer) SetCheckSum() {
	globBuf.mu.Lock()
	defer globBuf.mu.Unlock()

	checksum := globBuf.calCheckSum()

	// Guardamos el checksum en el espacio [0:4] definido por las constantes
	binary.LittleEndian.PutUint32(globBuf.buf[field_IndexCheckSumInit:field_IndexCheckSumEnd], checksum)
}

// GetCheckSum lee el checksum guardado en index
func (globBuf *indexBuffer) GetCheckSum() uint32 {
	globBuf.mu.RLock()
	defer globBuf.mu.RUnlock()
	// Leer el checksum guardado en el espacio [0:4]
	return binary.LittleEndian.Uint32(globBuf.buf[field_IndexCheckSumInit:field_IndexCheckSumEnd])
}

// SetSequence asigna la secuencia (8 bytes) en la posición correspondiente
func (globBuf *indexBuffer) SetSequence(seq int64) {
	globBuf.mu.Lock()
	defer globBuf.mu.Unlock()
	binary.LittleEndian.PutUint64(globBuf.buf[field_IndexSequenceInit:field_IndexSequenceEnd], uint64(seq))
}

func (globBuf *indexBuffer) GetSequence() int64 {
	globBuf.mu.RLock()
	defer globBuf.mu.RUnlock()
	return BytesToInt64(globBuf.buf[field_IndexSequenceInit:field_IndexSequenceEnd])
}

// SetSizePagination define el tamaño de la página en múltiplos de 4 (escribe 4 bytes)
func (globBuf *indexBuffer) SetSizePagination(size uint32) {
	globBuf.mu.Lock()
	defer globBuf.mu.Unlock()
	binary.LittleEndian.PutUint32(globBuf.buf[field_IndexSizePaginationInit:field_IndexSizePaginationEnd], size)
}

// GetSizePagination obtiene el tamaño de la página (lee 4 bytes)
func (globBuf *indexBuffer) GetSizePagination() uint32 {
	globBuf.mu.RLock()
	defer globBuf.mu.RUnlock()
	return binary.LittleEndian.Uint32(globBuf.buf[field_IndexSizePaginationInit:field_IndexSizePaginationEnd])
}

func (globBuf *indexBuffer) SetIndexKept(id int) {
	globBuf.mu.Lock()
	defer globBuf.mu.Unlock()

	if id > MaxSubIndexPerIndex {
		panic(errSubIndexOverFlow)
	}

	blockSubIndexActive := globBuf.buf[field_IndexKeptInit:field_IndexKeptEnd]
	blockSubIndexActive[id] = 1
}

func (globBuf *indexBuffer) UnSetIndexKept(id int) {
	globBuf.mu.Lock()
	defer globBuf.mu.Unlock()

	if id > MaxSubIndexPerIndex {
		panic(errSubIndexOverFlow)
	}

	blockSubIndexActive := globBuf.buf[field_IndexKeptInit:field_IndexKeptEnd]
	blockSubIndexActive[id] = 0
}

func (globBuf *indexBuffer) IsIndexKept(id int) bool {
	globBuf.mu.RLock()
	defer globBuf.mu.RUnlock()

	if id >= MaxSubIndexPerIndex {
		panic(errSubIndexOverFlow)
		return false
	}

	blockSubIndexActive := globBuf.buf[field_IndexKeptInit:field_IndexKeptEnd]
	return blockSubIndexActive[id] == 1
}

func (globBuf *indexBuffer) GetFirstEmptyIndex() (id int, found bool) {
	globBuf.mu.RLock()
	defer globBuf.mu.RUnlock()

	blockSubIndexActive := globBuf.buf[field_IndexKeptInit:field_IndexKeptEnd]

	// Recorremos los índices desde 0 hasta el límite MaxSubIndexPerIndex
	for id := 0; id <= MaxSubIndexPerIndex; id++ {

		// Control de seguridad por si el tamaño del slice es menor que MaxSubIndexPerIndex
		if id >= len(blockSubIndexActive) {
			break
		}

		// Si el byte es 0, significa que la posición está libre
		if blockSubIndexActive[id] == 0 {
			return id, true
		}
	}

	// Si no hay ningún espacio vacío, devolvemos -1 y false
	return -1, false
}
func (globBuf *indexBuffer) CountEmptyIndex() int {
	globBuf.mu.RLock()
	defer globBuf.mu.RUnlock()

	blockSubIndexActive := globBuf.buf[field_IndexKeptInit:field_IndexKeptEnd]
	count := 0

	// Recorremos los índices desde 0 hasta el límite MaxSubIndexPerIndex
	for id := 0; id <= MaxSubIndexPerIndex; id++ {

		// Control de seguridad por si el tamaño del slice es menor que MaxSubIndexPerIndex
		if id >= len(blockSubIndexActive) {
			break
		}

		// Si el byte es 0, el espacio está libre y sumamos al contador
		if blockSubIndexActive[id] == 0 {
			count++
		}
	}

	return count
}

func (globBuf *indexBuffer) GetHashSearch() [32]byte {
	globBuf.mu.RLock()
	defer globBuf.mu.RUnlock()

	bufferActive := globBuf.buf[field_HashSearchInit:field_HashSearchEnd]

	return [32]byte(bufferActive)
}

func (globBuf *indexBuffer) SetHashSearch() [32]byte {
	globBuf.mu.Lock()
	defer globBuf.mu.Unlock()

	hash := NewUUIDBytes()

	bufferActive := globBuf.buf[field_HashSearchInit:field_HashSearchEnd]

	copy(bufferActive, hash[:])

	return hash
}

func (globBuf *indexBuffer) UnSetHashSearch() {
	globBuf.mu.Lock()
	defer globBuf.mu.Unlock()

	bufferActive := globBuf.buf[field_HashSearchInit:field_HashSearchEnd]
	clear(bufferActive)
	return
}

// GetMetadata devuelve todos los campos directamente usando tus funciones Get existentes
func (globBuf *indexBuffer) GetMetadata() (checkSum uint32, sizePagination uint32, sequence int64, hash [32]byte) {
	return globBuf.GetCheckSum(), globBuf.GetSizePagination(), globBuf.GetSequence(), globBuf.GetHashSearch()
}
