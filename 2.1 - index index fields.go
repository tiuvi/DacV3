package dacV3

import (
	"encoding/binary"
	"hash/crc32"

)

func (b indexBuffer) CalCheckSum() uint32 {
	// Se calcula el checksum desde el final del checksum hasta el límite de la página
	return crc32.Checksum(b[field_IndexCheckSumEnd:BufferAlignSize], castagnoliTable)
}

// SetCheckSum calcula el checksum de data y lo escribe en la sección de checksum de index
func (b indexBuffer) SetCheckSum() {
	checksum := b.CalCheckSum()

	// Guardamos el checksum en el espacio [0:4] definido por las constantes
	binary.BigEndian.PutUint32(b[field_IndexCheckSumInit:field_IndexCheckSumEnd], checksum)
}

// GetCheckSum lee el checksum guardado en index
func (b indexBuffer) GetCheckSum() uint32 {
	// Leer el checksum guardado en el espacio [0:4]
	return binary.BigEndian.Uint32(b[field_IndexCheckSumInit:field_IndexCheckSumEnd])
}


// SetSequence asigna la secuencia (8 bytes) en la posición correspondiente
func (b indexBuffer) SetSequence(seq int64) {
	binary.BigEndian.PutUint64(b[field_IndexSequenceInit:field_IndexSequenceEnd], uint64(seq))
}

func (b indexBuffer) GetSequence() int64 {
	return BytesToInt64(b[field_IndexSequenceInit:field_IndexSequenceEnd])
}

// SetSizePagination define el tamaño de la página en múltiplos de 4 (escribe 4 bytes)
func (b indexBuffer) SetSizePagination(size uint32) {
	binary.BigEndian.PutUint32(b[field_IndexSizePaginationInit:field_IndexSizePaginationEnd], size)
}

// GetSizePagination obtiene el tamaño de la página (lee 4 bytes)
func (b indexBuffer) GetSizePagination() uint32 {
	return binary.BigEndian.Uint32(b[field_IndexSizePaginationInit:field_IndexSizePaginationEnd])
}

func (b indexBuffer) SetIndexKept(id int) {

	if id > maxSubIndexPerIndex {
		panic(errSubIndexOverFlow)
	}

	blockSubIndexActive := b[field_IndexKeptInit:field_IndexKeptEnd]
	blockSubIndexActive[id] = 1
}

func (b indexBuffer) UnSetIndexKept(id int) {

	if id > maxSubIndexPerIndex {
		panic(errSubIndexOverFlow)
	}

	blockSubIndexActive := b[field_IndexKeptInit:field_IndexKeptEnd]
	blockSubIndexActive[id] = 0
}

func (b indexBuffer) IsIndexKept(id int) bool {

	if id > maxSubIndexPerIndex {
		panic(errSubIndexOverFlow)
		return false
	}

	blockSubIndexActive := b[field_IndexKeptInit:field_IndexKeptEnd]
	return blockSubIndexActive[id] == 1
}

func (b indexBuffer) GetFirstEmptyIndex() (id int, found bool) {

	blockSubIndexActive := b[field_IndexKeptInit:field_IndexKeptEnd]

	// Recorremos los índices desde 0 hasta el límite maxSubIndexPerIndex
	for id := 0; id <= maxSubIndexPerIndex; id++ {

		// Control de seguridad por si el tamaño del slice es menor que maxSubIndexPerIndex
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

func (b indexBuffer) GetHashSearch() [32]byte{

	bufferActive := b[field_HashSearchInit:field_HashSearchEnd]

	return [32]byte(bufferActive)
}

func (b indexBuffer) SetHashSearch() [32]byte {

	hash := NewUUIDBytes()

	bufferActive := b[field_HashSearchInit:field_HashSearchEnd]

	copy(bufferActive, hash[:])

	return hash
}

func (b indexBuffer) UnSetHashSearch() {

	bufferActive := b[field_HashSearchInit:field_HashSearchEnd]
	clear(bufferActive)
	return
}

// GetMetadata devuelve todos los campos directamente usando tus funciones Get existentes
func (b indexBuffer) GetMetadata() (checkSum uint32, sizePagination uint32, sequence int64, hash [32]byte) {
	return  b.GetCheckSum() ,b.GetSizePagination(), b.GetSequence(), b.GetHashSearch()
}

