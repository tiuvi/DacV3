package main

import (
	"encoding/binary"
	"errors"
	"sync"
)

func calcIndexSize(size int64) int64 {

	//primero calculamos el tamaño de las paginas
	return size * 51
}

// Indice
type Index struct {
	mu               sync.Mutex
	idLocationBuffer int64
	idLocationIndex  int64
	offset           int64
	sizePagination   int64
	sequence         int64
}

// Paginas de buffer de indices

type indexBuffer []byte

// Asigna el tamaño de paginación (8 bytes) en la posición 1:9
func (b indexBuffer) SetSizePagination(size int64) {

	binary.BigEndian.PutUint64(b[0:8], uint64(size))
}

func (b indexBuffer) GetSizePagination() int64 {

	return BytesToInt64(b[0:8])
}

// Asigna la secuencia (8 bytes) en la posición 9:17
func (b indexBuffer) SetSequence(seq int64) {
	binary.BigEndian.PutUint64(b[8:16], uint64(seq))
}

func (b indexBuffer) GetSequence() int64 {

	return BytesToInt64(b[8:16])
}

// Asigna el hash (32 bytes) en la posición 17:49
func (b indexBuffer) SetHash() {

	hash := NewUUIDSheedBytes(b[100:4096])

	copy(b[16:48], hash[:])
}

// Devuelve una vista del hash guardado en el buffer
func (b indexBuffer) GetHash() [32]byte {
	return [32]byte(b[16:48])
}

func (b indexBuffer) CalcHash() [32]byte {

	return NewUUIDSheedBytes(b[100:4096])
}

// GetMetadata devuelve todos los campos directamente usando tus funciones Get existentes
func (b indexBuffer) GetMetadata() (sizePagination int64, sequence int64, hash [32]byte) {
	return b.GetSizePagination(), b.GetSequence(), b.GetHash()
}

func (sfDacV3 *dacV3) newIndex(offset int64, sizePagination int64) (err error) {

	idIndex, index := sfDacV3.indexLocation.New()

	index.mu.Lock()
	defer index.mu.Unlock()

	idBuffer, buf := sfDacV3.indexBuffer.addBufferArena()

	bufIndex := indexBuffer(buf)

	bufIndex.SetSizePagination(sizePagination)

	bufIndex.SetHash()

	bufIndex.SetSequence(1)

	index.idLocationBuffer = idBuffer
	index.idLocationIndex = idIndex
	index.offset = offset
	index.sizePagination = sizePagination
	index.sequence = 1

	_, err = sfDacV3.file.WriteAt(buf[0:4096], offset)
	if err != nil {
		return err
	}

	indexChan, exists := sfDacV3.indexPools[sizePagination]
	if !exists {
		return errors.New("Ese tamaño de indice no existe")
	}

	indexChan <- idIndex

	return
}

func (sfDacV3 *dacV3) initIndex(offset int64) (err error) {

	idIndex, index := sfDacV3.indexLocation.New()

	index.mu.Lock()
	defer index.mu.Unlock()

	// 1. Solicitamos un slot en la arena de buffers
	idBuffer, buf := sfDacV3.indexBuffer.addBufferArena()

	index.idLocationBuffer = idBuffer
	index.idLocationIndex = idIndex

	// 2. Leemos los 4096 bytes directamente desde el disco en el offset indicado
	_, err = sfDacV3.file.ReadAt(buf, offset)
	if err != nil {
		return err
	}

	indexBuffer1 := indexBuffer(buf[0:4096])

	sizePagination1, sequence1, hash1 := indexBuffer1.GetMetadata()

	indexBuffer2 := indexBuffer(buf[4096:8192])

	sizePagination2, sequence2, hash2 := indexBuffer2.GetMetadata()

	if sequence1 > sequence2 {

		//Bloque correcto
		if hash1 == indexBuffer1.CalcHash() {

			index.offset = offset
			index.sizePagination = sizePagination1
			index.sequence = sequence1

			return
		}
	}

	//Apartir de aqui si el bloque no es correcto
	if hash2 == indexBuffer2.CalcHash() {

		index.offset = offset
		index.sizePagination = sizePagination2
		index.sequence = sequence2

		return
	}

	return errors.New("indices corruptos")
}

// ESta funcion actua dentro de un bloqueo
func (sfDacV3 *dacV3) updateIndex(index *Index) (err error) {

	buf := sfDacV3.indexBuffer.getBufferArena(index.idLocationBuffer)

	// Calculamos cuál va a ser la siguiente secuencia
	index.sequence = index.sequence + 1

	bufIndex := indexBuffer(buf[0:4096])

	// Asignamos la nueva secuencia al buffer
	bufIndex.SetSequence(index.sequence)

	// Calculamos el nuevo hash basado en los datos actuales del bloque y lo guardamos
	bufIndex.SetHash()

	// Escribimos en el disco exactamente el Bloque 1
	_, err = sfDacV3.file.WriteAt(buf[0:4096], index.offset)
	if err != nil {
		return err
	}

	return nil
}
