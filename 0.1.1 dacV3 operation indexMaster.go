package dacV3

import (
	"errors"
	"log"
)

// Set marca un byte completo como ocupado (0xFF)
func (b *indexMaster) Set(blockID int) (segmentIndex int, byteIndex int, alignedOffset4K int) {

	segmentIndex = blockID / b.opts.sizeIndexMaster

	byteIndex = blockID % b.opts.sizeIndexMaster

	// Asignamos el byte completo como ocupado (11111111)
	b.globalSize[segmentIndex][byteIndex] = 0xFF

	// Calculamos el bloque de 4096 al que pertenece este byte para O_DIRECT
	// Como sizeIndexMaster es 1024, esto siempre dará 0, pero la fórmula te sirve
	// si en el futuro amplías tu mapa a más de 4096 bytes.
	alignedOffset4K = (byteIndex / 4096) * 4096

	return
}

// UnSet marca un byte completo como libre (0x00)
func (b *indexMaster) UnSet(blockID int) (segmentIndex int, byteIndex int, alignedOffset4K int) {

	segmentIndex = blockID / b.opts.sizeIndexMaster
	byteIndex = blockID % b.opts.sizeIndexMaster

	// Asignamos el byte completo como libre (00000000)
	b.globalSize[segmentIndex][byteIndex] = 0x00

	alignedOffset4K = (byteIndex / 4096) * 4096

	return
}

// Get lee si el byte está ocupado (true) o libre (false)
func (b *indexMaster) Get(blockID int) bool {

	segmentIndex := blockID / b.opts.sizeIndexMaster
	byteIndex := blockID % b.opts.sizeIndexMaster

	// Retorna true si el byte está ocupado
	return b.globalSize[segmentIndex][byteIndex] == 0xFF
}

// GetBytesOff busca 'n' bytes consecutivos libres (0x00)
// Esto reemplaza a GetBitsOff y es inmensamente más rápido
func (b *indexMaster) GetBytesOff(n int) (id int, found bool) {

	start := -1
	count := 0

	totalSegments := len(b.globalSize)

	// Iteramos segmento por segmento
	for seg := 0; seg < totalSegments; seg++ {

		//Reiniciamos a cada segmento para no obtener bytes de segmentos distintos
		start = -1
		count = 0

		// Iteramos byte por byte
		for by := 0; by < b.opts.sizeIndexMaster; by++ {

			val := b.globalSize[seg][by]

			if val == 0x00 { // Si el byte está completamente libre
				if start == -1 {
					// Guardamos el ID base absoluto
					start = (seg * b.opts.sizeIndexMaster) + by
				}
				count++

				// Si ya encontramos la cantidad solicitada
				if count == n {
					return start, true
				}
			} else {
				// Si nos chocamos con un byte ocupado (0xFF), reiniciamos la cuenta
				start = -1
				count = 0
			}
		}
	}

	// No hay espacio suficiente contiguo
	return 0, false
}

func newIndex(sfDacV3 *dacV3, offset int64, sizePagination uint32) (idIndex uint32, buf []byte) {

	idIndex, index := sfDacV3.indexLocation.New()

	idBuffer, buf := sfDacV3.indexBuffer.addBufferArena()

	bufIndex := indexBuffer(buf)

	bufIndex.SetSizePagination(sizePagination)

	bufIndex.SetSequence(1)

	bufIndex.SetCheckSum()

	index.idLocationBuffer = idBuffer

	index.offset = offset

	return
}

func (sfDacV3 *dacV3) newIndexs(nIndex int64, sizePagination int64) (idsIndex []uint32, err error) {

	sfDacV3.mu.Lock()
	defer sfDacV3.mu.Unlock()

	walSumBuffersSize := sfDacV3.dacV3WorkerWriter.walSumBuffersSize

	// 1. Tamaño que representa 1 BIT (e.g. 4096 bytes)
	baseUnitSize := int64(sfDacV3.indexMaster.blockMinSize.pageSize)

	// 2. Validación de que es múltiplo
	if sizePagination%baseUnitSize != 0 {
		return nil, errors.New("tamaño de pagina no compatible")
	}

	relationWithMinBlock := sizePagination / baseUnitSize

	// 3. TU BUCLE OPTIMIZADO (Bulk Allocation Math)
	manyBytes := 0
	totalIndex := 0

	// Empezamos en 1 para evitar que el 0 de un falso positivo inmediato
	for ind := 1; ; ind++ {

		indexPerByteValidation := (8 * ind) % int(relationWithMinBlock)

		if indexPerByteValidation != 0 {
			continue // Aún no cuadra de forma exacta
		}

		// Encontramos la coincidencia perfecta (Mínimo Común Múltiplo)
		manyBytes = ind
		totalIndex = (8 * ind) / int(relationWithMinBlock)
		break // Salimos del bucle
	}

	if int64(totalIndex) < nIndex {

		// Fórmula mágica para redondear divisiones de enteros hacia arriba: (A + B - 1) / B
		multiplier := int((nIndex + int64(totalIndex) - 1) / int64(totalIndex))

		manyBytes *= multiplier

		totalIndex *= multiplier
	}

	if manyBytes > sfDacV3.indexMaster.opts.sizeIndexMaster {
		// Si en el futuro necesitas más, tendrás que hacer un bucle externo llamando a reserveSize varias veces
		return nil, errors.New("se han solicitado demasiados bloques de golpe y exceden la capacidad contigua de un solo segmento")
	}

	// 4. Buscar espacio libre en el mapa (manyBytes)
	startBlockID, found := sfDacV3.indexMaster.GetBytesOff(manyBytes)
	if !found {
		// No hay espacio en los segmentos actuales. Creamos uno nuevo dinámicamente.

		// 1. Añadimos un nuevo mapa de bits vacío a la memoria RAM
		newBuffer := MakeAlignedBlock(sfDacV3.indexMaster.opts.sizeIndexMaster)
		sfDacV3.indexMaster.globalSize = append(sfDacV3.indexMaster.globalSize, newBuffer)

		// 2. Calculamos el nuevo tamaño físico que debe tener el archivo
		newSegmentsCount := len(sfDacV3.indexMaster.globalSize)

		newFileSize := walSumBuffersSize + int64(newSegmentsCount)*sfDacV3.indexMaster.segmentPhysicalSize

		// 3. Expandimos el archivo en el disco
		sfDacV3.ExpandSize(newFileSize)

		// 4. Relanzamos la búsqueda (ahora garantizado que habrá espacio)
		startBlockID, found = sfDacV3.indexMaster.GetBytesOff(manyBytes)
		if !found {
			log.Fatalln("error critico: sin espacio tras expandir el archivo")
		}
	}

	var initSegIndex int
	var initIdIndex int
	var initAlignedOffset4K int

	var lastAlignedOffset4K int

	// 5. Marcar los bytes requeridos como ocupados (0xFF)
	for i := 0; i < manyBytes; i++ {

		currentBlockID := startBlockID + i

		segIndex, idIndex, alignedOffset4K := sfDacV3.indexMaster.Set(currentBlockID)

		if i == 0 {
			initSegIndex = segIndex
			initIdIndex = idIndex
			initAlignedOffset4K = alignedOffset4K
		}

		lastAlignedOffset4K = alignedOffset4K
	}

	//Buffer exacto de los bytes actualizado
	bufIndexMaster := sfDacV3.indexMaster.globalSize[initSegIndex][initAlignedOffset4K : lastAlignedOffset4K+4096]

	OffsetIndexMaster := walSumBuffersSize + (int64(initSegIndex) * sfDacV3.indexMaster.segmentPhysicalSize) + int64(initAlignedOffset4K)

	//calculando el offset de los indices
	idsIndex = make([]uint32, totalIndex)

	// 1. Dónde empieza el bloque físico entero de este segmento (Absoluto)
	segmentOffset := walSumBuffersSize + int64(initSegIndex)*sfDacV3.indexMaster.segmentPhysicalSize

	// 2. El tamaño del mapa que debemos "saltar" para llegar a los datos
	headerMapSize := int64(sfDacV3.indexMaster.opts.sizeIndexMaster)

	// 3. Dónde empiezan los datos relativos a este byte dentro del segmento
	dataOffsetInSegment := int64(initIdIndex) * sfDacV3.indexMaster.idIndexPhysicalSizePerByte

	// 4. Base absoluta donde empezaremos a escribir los nuevos índices
	// (Inicio Segmento + Salto del Mapa + Posición del Dato)
	baseDataOffset := segmentOffset + headerMapSize + dataOffsetInSegment

	for ind := range totalIndex {
		// --- DENTRO DEL BUCLE: Solo calculamos lo que cambia (el indexOffset) ---

		// Calculamos el desplazamiento del indice específico
		indexOffset := int64(ind) * sfDacV3.indexMaster.sizeSubIndex[uint32(sizePagination)].sizeBlock

		// Offset final absoluto en disco
		fisrtOffsetIndex := baseDataOffset + indexOffset

		idIndex, buf := newIndex(sfDacV3, fisrtOffsetIndex, uint32(sizePagination))

		sfDacV3.WriteIndex(buf, fisrtOffsetIndex)

		idsIndex[ind] = idIndex
	}

	err = sfDacV3.WriteIndexMaster(bufIndexMaster , OffsetIndexMaster)
	if err != nil {
		return nil, err
	}

	return idsIndex, nil
}
