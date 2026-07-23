package dacV3

import (
	"errors"
)

var errIndexCorrupt = errors.New("indice corrupto")

// loadAndVerifyIndexBlock (Helper) Centraliza la lógica de lectura y validación de bloques A/B
func (sfDacV3 *DacV3) loadAndVerifyIndexBlock(offset int64, targetBuf []byte) (uint32, [32]byte, error) {

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
func (sfDacV3 *DacV3) initIndex(offset int64) (idIndex uint32, sizePagination uint32, hash [32]byte, slotsFree int64, index *Index, err error) {

	idIndex, index = sfDacV3.indexLocation.New()

	idBuffer, buf := sfDacV3.indexBuffer.addBufferArena()

	index.idLocationBuffer = idBuffer

	index.offset = offset

	sizePagination, hash, err = sfDacV3.loadAndVerifyIndexBlock(offset, buf)
	if err != nil {
		return 0, 0, [32]byte{}, 0, nil, err
	}

	slotsFree = int64(indexBuffer(buf).CountEmptyIndex())

	return idIndex, sizePagination, hash, slotsFree, index, nil
}

// LoadIndex Refresca el buffer del índice
// Nota: Se ha añadido el retorno de 'error' para no fallar silenciosamente si hay corrupción.
func (sfDacV3 *DacV3) LoadIndex(index *Index) (indexBuffer, error) {

	index.mu.Lock()
	defer index.mu.Unlock()

	if index.idLocationBuffer != 0 {
		return sfDacV3.indexBuffer.getBufferArena(index.idLocationBuffer), nil
	}

	idBuffer, buf := sfDacV3.indexBuffer.addBufferArena()

	index.idLocationBuffer = idBuffer

	_, _, err := sfDacV3.loadAndVerifyIndexBlock(index.offset, buf)

	return buf, err // Si no puedes cambiar la firma de la función para devolver error, puedes ignorarlo aquí.
}

func newIndex(sfDacV3 *DacV3, offset int64, sizePagination uint32) (idIndex uint32) {

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

func newIndexSearch(sfDacV3 *DacV3, offset int64, sizePagination uint32) (idIndex uint32) {

	idIndex, index := sfDacV3.indexLocation.New()

	idBuffer, buf := sfDacV3.indexBuffer.addBufferArena()

	bufIndex := indexBuffer(buf)

	bufIndex.SetSizePagination(sizePagination)

	// Iniciamos en 1 (Secuencia Impar). Esto significa que siempre
	// se escribirá por primera vez en el Bloque 1 (offset + 0)
	bufIndex.SetSequence(1)

	hash := bufIndex.SetHashSearch()

	//Añadimos al mapa de hash
	sfDacV3.indexSearch[hash] = IndexSearch{
		offset:           index.offset,
		idLocationBuffer: index.idLocationBuffer,
	}

	bufIndex.SetCheckSum()

	index.idLocationBuffer = idBuffer

	// Guardamos el offset BASE del índice en la estructura
	index.offset = offset

	// Como sabemos que la secuencia es 1, escribimos directamente en el offset base
	sfDacV3.WriteIndex(buf, offset)

	return idIndex
}

func (sfDacV3 *DacV3) updateIndex(index *Index) {

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

func (sfDacV3 *DacV3) getOffsetPageStart(index *Index, page *Page) int64 {

	buf := sfDacV3.indexBuffer.getBufferArena(index.idLocationBuffer)

	bufIndex := indexBuffer(buf)

	config := sfDacV3.globalSizeSubIndex[bufIndex.GetSizePagination()]

	return index.offset + config.pageStartOffset + (int64(page.idSubIndex) * config.pageSize)
}

type indexHandle struct {
	*Index
	indexBuffer
}

func (sfDacV3 *DacV3) newIndexHandle(idIndex uint32) (*indexHandle, error) {

	index := sfDacV3.indexLocation.Get(idIndex)

	buf, err := sfDacV3.LoadIndex(index)
	if err != nil {
		return nil, err
	}

	return &indexHandle{
		Index:       index,
		indexBuffer: buf,
	}, nil

}

var ErrNoSpaceAllocated = errors.New("no se pudo asignar espacio tras agotar todos los intentos y tamaños")

func (sfDacV3 *DacV3) GetSizeForIndex(requiredSpace uint32) (size uint32) {

	for _, size := range sfDacV3.globalSizeIndex {

		if size < requiredSpace {
			continue
		}

		return size
	}

	return 0
}

func (sfDacV3 *DacV3) CreatePageInIndex(hash [32]byte, requiredSpace uint32) (sfIndexHandle *indexHandle, newIdIndex uint32, newIdSubIndex uint8, err error) {

	size := sfDacV3.GetSizeForIndex(requiredSpace)
	if size == 0 {
		return nil, 0, 0, ErrNoSpaceAllocated
	}

	pool := sfDacV3.indexPools[size]
	success := false

	for intento := 0; intento < 100; intento++ {

		select {

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

				sfDacV3.needIndexChan <- newIndexRequest{
					sizePagination: int64(size),
					isSearch:       false,
				}

			}

			sfIndexHandle, err = sfDacV3.newIndexHandle(idIndex.IDIndex)
			if err != nil {
				// Aquí sí usamos fmt.Errorf para añadir contexto dinámico (el idIndex)
				return nil, 0, 0, err
			}

			sfIndexHandle.mu.Lock()

			id, found := sfIndexHandle.GetFirstEmptyIndex()
			if !found {
				sfIndexHandle.mu.Unlock()
				continue
			}

			newIdSubIndex = uint8(id)

			newIdIndex = idIndex.IDIndex

			sfIndexHandle.SetSubIndexSequence(id, 0)

			sfIndexHandle.setSubIndex(hash, id)

			sfIndexHandle.mu.Unlock()

			success = true

		default:

			//Este bloqueo es lento no interesa que se agrupoen aqui las gorutinas
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

// Funcion para obtener el siguiente indice
func (sfDacV3 *DacV3) UpdatePageInIndex(sfIndexHandleCurrent *indexHandle, idSubIndexCurrent uint8, requiredSpace uint32) (sfIndexHandle *indexHandle, newIdIndex uint32, newIdSubIndex uint8, err error) {

	println("Esto no tiene que funcionar todavia....")
	size := sfDacV3.GetSizeForIndex(requiredSpace)
	if size == 0 {
		return nil, 0, 0, ErrNoSpaceAllocated
	}

	pool := sfDacV3.indexPools[size]
	success := false

	for intento := 0; intento < 10; intento++ {

		select {

		case idIndex := <-pool:

			//Una vez que obtenenemos el indice no lo necesitamos lo enviamos al pool.
			//FAlta hacer un bloqueo para el buffer y otro para la escritura de indice.
			pool <- idIndex

			sfIndexHandle, err = sfDacV3.newIndexHandle(idIndex.IDIndex)
			if err != nil {
				// Aquí sí usamos fmt.Errorf para añadir contexto dinámico (el idIndex)
				return nil, 0, 0, err
			}

			sfIndexHandle.mu.Lock()

			id, found := sfIndexHandle.GetFirstEmptyIndex()
			if !found {
				// El índice está lleno.
				sfIndexHandle.mu.Unlock()
				continue
			}

			// ¡Éxito!
			// Se invierte el orden para asegurar happens-before sobre idSubIndex
			newIdSubIndex = uint8(id)

			newIdIndex = idIndex.IDIndex

			// Obtenemos el hash del índice antiguo de forma segura
			sfIndexHandleCurrent.mu.Lock()
			hash := sfIndexHandleCurrent.GetSubIndexHash(int(idSubIndexCurrent))
			sequence := sfIndexHandleCurrent.GetSubIndexSequence(int(idSubIndexCurrent))
			sfIndexHandleCurrent.mu.Unlock()

			sfIndexHandle.SetSubIndexSequence(id, sequence+1)

			sfIndexHandle.setSubIndex(hash, id)

			sfIndexHandle.mu.Unlock()

			success = true

		default:
			//Hace falta un atomico para que cuente los indices disponibles
			//Si el atomico esta por encima de los indices que puede a ver entonces no crear indices y esperar.
			// El canal está vacío, creamos 1 índice nuevo
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
