package dacV3

import (
	"errors"
)

var errIndexCorrupt = errors.New("indice corrupto")

// loadAndVerifyIndexBlock (Helper) Centraliza la lógica de lectura y validación de bloques A/B
func (sfDacV3 *dacV3) loadAndVerifyIndexBlock(offset int64, targetBuf []byte) (uint32, [32]byte, error) {

	// Leer y validar Bloque 1
	sfDacV3.ReadAt(targetBuf, offset)

	block1 := indexBuffer(targetBuf)

	chk1, size1, seq1, hash1 := block1.GetMetadata()

	valid1 := chk1 == block1.CalCheckSum()

	// Leer y validar Bloque 2 (Respaldo)
	buf2 := MakeAlignedBlock(BufferAlignSize)

	sfDacV3.ReadAt(buf2, offset+BufferAlignSize) // ¡Bug corregido: Ahora se lee en buf2!

	block2 := indexBuffer(buf2)

	chk2, size2, seq2, hash2 := block2.GetMetadata()

	valid2 := chk2 == block2.CalCheckSum()

	// Lógica de decisión mejorada
	if valid1 && (!valid2 || seq1 > seq2) {
		// El bloque 1 es válido y es el más reciente (o el bloque 2 está corrupto)
		return size1, hash1, nil
	}

	if valid2 {

		// El bloque 2 es válido (y es el más reciente, o el bloque 1 estaba corrupto)
		copy(targetBuf, buf2)

		return size2, hash2, nil
	}

	// Ambos bloques están corruptos
	return 0, [32]byte{}, errIndexCorrupt
}

// initIndex Inicializa los índices y todas las páginas de índice
func (sfDacV3 *dacV3) initIndex(offset int64) (idIndex uint32, sizePagination uint32, hash [32]byte, index *Index, err error) {

	idIndex, index = sfDacV3.indexLocation.New()

	idBuffer, buf := sfDacV3.indexBuffer.addBufferArena()

	index.idLocationBuffer = idBuffer

	index.offset = offset

	sizePagination, hash, err = sfDacV3.loadAndVerifyIndexBlock(offset, buf)
	if err != nil {
		return 0, 0, [32]byte{}, nil, err
	}

	return idIndex, sizePagination, hash, index, nil
}

// LoadIndex Refresca el buffer del índice
// Nota: Se ha añadido el retorno de 'error' para no fallar silenciosamente si hay corrupción.
func (sfDacV3 *dacV3) LoadIndex(index *Index) error {

	index.mu.Lock()
	defer index.mu.Unlock()

	idBuffer, buf := sfDacV3.indexBuffer.addBufferArena()

	index.idLocationBuffer = idBuffer

	_, _, err := sfDacV3.loadAndVerifyIndexBlock(index.offset, buf)

	return err // Si no puedes cambiar la firma de la función para devolver error, puedes ignorarlo aquí.
}

func newIndex(sfDacV3 *dacV3, offset int64, sizePagination uint32) (idIndex uint32) {

	idIndex, index := sfDacV3.indexLocation.New()

	idBuffer, buf := sfDacV3.indexBuffer.addBufferArena()

	bufIndex := indexBuffer(buf)

	bufIndex.SetSizePagination(sizePagination)

	// Iniciamos en 1 (Secuencia Impar). Esto significa que siempre
	// se escribirá por primera vez en el Bloque 1 (offset + 0)
	bufIndex.SetSequence(1)

	bufIndex.SetCheckSum()

	index.idLocationBuffer = idBuffer

	// Guardamos el offset BASE del índice en la estructura
	index.offset = offset

	// Como sabemos que la secuencia es 1, escribimos directamente en el offset base
	sfDacV3.WriteIndex(buf, offset)

	return idIndex
}

func newIndexSearch(sfDacV3 *dacV3, offset int64, sizePagination uint32) (idIndex uint32) {

	idIndex, index := sfDacV3.indexLocation.New()

	idBuffer, buf := sfDacV3.indexBuffer.addBufferArena()

	bufIndex := indexBuffer(buf)

	bufIndex.SetSizePagination(sizePagination)

	// Iniciamos en 1 (Secuencia Impar). Esto significa que siempre
	// se escribirá por primera vez en el Bloque 1 (offset + 0)
	bufIndex.SetSequence(1)

	bufIndex.SetHashSearch()

	bufIndex.SetCheckSum()

	index.idLocationBuffer = idBuffer

	// Guardamos el offset BASE del índice en la estructura
	index.offset = offset

	// Como sabemos que la secuencia es 1, escribimos directamente en el offset base
	sfDacV3.WriteIndex(buf, offset)

	return idIndex
}

func (sfDacV3 *dacV3) updateIndex(index *Index) {

	buf := sfDacV3.indexBuffer.getBufferArena(index.idLocationBuffer)

	bufIndex := indexBuffer(buf)

	// Incrementamos la secuencia
	sequence := bufIndex.GetSequence() + 1

	// Asignamos la nueva secuencia al buffer
	bufIndex.SetSequence(sequence)

	// Calculamos el nuevo checksum DESPUÉS de modificar la secuencia
	bufIndex.SetCheckSum()

	// LÓGICA DE ALTERNANCIA (PING-PONG) usando módulo %
	// Empezamos asumiendo que es el Bloque 1 (impar)
	targetOffset := index.offset

	// Si la secuencia es par, cambiamos al Bloque 2 sumando 4096
	if sequence%2 == 0 {
		targetOffset += BufferAlignSize
	}

	// Escribimos en el bloque alternado que corresponda
	// (Asegúrate de que sfDacV3.WriteIndex devuelva un error en tu código real para poder retornarlo)
	sfDacV3.WriteIndex(buf, targetOffset)

	return
}

func (sfDacV3 *dacV3) getOffsetPageStart(index *Index, page *Page) int64 {

	buf := sfDacV3.indexBuffer.getBufferArena(index.idLocationBuffer)

	bufIndex := indexBuffer(buf)

	config := sfDacV3.globalSizeSubIndex[bufIndex.GetSizePagination()]

	return index.offset + config.pageStartOffset + (int64(page.idSubIndex) * config.pageSize)
}
