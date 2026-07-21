package dacV3

import (
	"errors"
)

// Set marca un byte completo como ocupado (0xFF)
func (b *indexMaster) Set(blockID int) (segmentIndex int, byteIndex int, alignedOffset4K int) {

	segmentIndex = blockID / b.opts.SizeIndexMaster

	byteIndex = blockID % b.opts.SizeIndexMaster

	// Asignamos el byte completo como ocupado (11111111)
	b.globalSize[segmentIndex][byteIndex] = 0xFF

	// Calculamos el bloque de 4096 al que pertenece este byte para O_DIRECT
	// Como SizeIndexMaster es 1024, esto siempre dará 0, pero la fórmula te sirve
	// si en el futuro amplías tu mapa a más de 4096 bytes.
	alignedOffset4K = (byteIndex / 4096) * 4096

	return
}

// UnSet marca un byte completo como libre (0x00)
func (b *indexMaster) UnSet(blockID int) (segmentIndex int, byteIndex int, alignedOffset4K int) {

	segmentIndex = blockID / b.opts.SizeIndexMaster
	byteIndex = blockID % b.opts.SizeIndexMaster

	// Asignamos el byte completo como libre (00000000)
	b.globalSize[segmentIndex][byteIndex] = 0x00

	alignedOffset4K = (byteIndex / 4096) * 4096

	return
}

// Get lee si el byte está ocupado (true) o libre (false) y retorna su offset absoluto en disco
func (b *indexMaster) Get(blockID int) (isOccupied bool, offset int64) {

	segmentIndex := blockID / b.opts.SizeIndexMaster
	byteIndex := blockID % b.opts.SizeIndexMaster

	// Calculamos el offset absoluto del primer bit de este byte
	offset = b.walSumBuffersSize +
		(int64(segmentIndex) * b.segmentPhysicalSize) +
		int64(b.opts.SizeIndexMaster) +
		(int64(byteIndex) * b.idIndexPhysicalSizePerByte)

	// Retorna true si el byte está completamente ocupado (0xFF)
	isOccupied = b.globalSize[segmentIndex][byteIndex] == 0xFF

	return isOccupied, offset
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
		for by := 0; by < b.opts.SizeIndexMaster; by++ {

			val := b.globalSize[seg][by]

			if val == 0x00 { // Si el byte está completamente libre
				if start == -1 {
					// Guardamos el ID base absoluto
					start = (seg * b.opts.SizeIndexMaster) + by
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

var errServerNotSizeAvaible = errors.New("server not size avaible") 
var errFileSizeLimitExceeded = errors.New("the file exceeds the server size limit, per file")

func (sfDacV3 *DacV3) newIndexs(nIndex int64, sizePagination int64, isSearch bool) (err error) {

	sfDacV3.mu.Lock()
	defer sfDacV3.mu.Unlock()

	if isSearch {

		if len(sfDacV3.indexSearchPool) > int(nIndex) {
			return
		}

		needed := int(sfDacV3.opts.NChanAvaibleIndexSearch) - len(sfDacV3.indexSearchPool)
		if needed > int(nIndex) {
			nIndex = int64(needed)
		}

	} else {

		//Tamaño del canal declarado
		data := sfDacV3.indexMaster.sizeSubIndex[uint32(sizePagination)]

		//Tamaño del pool
		pool := sfDacV3.indexPools[uint32(sizePagination)]
		if len(pool) > int(nIndex) {
			return
		}

		// Usar los valores en IndexSizeChan
		needed := int(data.IndexSizeChan) - len(pool)
		if needed > int(nIndex) {
			nIndex = int64(needed)
		}
	}

	walSumBuffersSize := sfDacV3.dacV3WorkerWriter.walSumBuffersSize

	// 1. Tamaño que representa 1 BIT (e.g. 4096 bytes)
	baseUnitSize := int64(sfDacV3.indexMaster.blockMinSize.pageSize)

	// 2. Validación de que es múltiplo
	if sizePagination%baseUnitSize != 0 {
		return errors.New("tamaño de pagina no compatible")
	}

	relationWithMinBlock := sizePagination / baseUnitSize

	// Calculamos cuántos bloques mínimos caben en el bloque máximo (ej. 64k / 4k = 16 bits)
	maxMinRelation := sfDacV3.indexMaster.maxMinRelationBlock

	// 3. TU BUCLE OPTIMIZADO (Bulk Allocation Math)
	manyBytes := 0
	totalIndex := 0

	// Empezamos en 1 para evitar que el 0 de un falso positivo inmediato
	for ind := 1; ; ind++ {

		// Total de bits (mínimos) que estamos evaluando en esta iteración
		totalBits := maxMinRelation * ind

		// 1. Validamos que los bits cuadren exactamente con la paginación solicitada
		if totalBits%int(relationWithMinBlock) != 0 {
			continue // Aún no cuadra de forma exacta con la paginación
		}

		// 2. Validamos que los bits conformen bytes completos (múltiplos de 8)
		if totalBits%8 != 0 {
			continue // Aún no conforma un byte completo
		}

		// Encontramos la coincidencia perfecta (Mínimo Común Múltiplo)
		// Convertimos los bits totales a bytes completos
		manyBytes = totalBits / 8
		totalIndex = totalBits / int(relationWithMinBlock)
		break // Salimos del bucle
	}

	if int64(totalIndex) < nIndex {

		// Fórmula mágica para redondear divisiones de enteros hacia arriba: (A + B - 1) / B
		multiplier := int((nIndex + int64(totalIndex) - 1) / int64(totalIndex))

		manyBytes *= multiplier

		totalIndex *= multiplier
	}

	if manyBytes > sfDacV3.indexMaster.opts.SizeIndexMaster {
		//Error aqui excede la capacidad por segmento, no se pueden optener bytes de varios segmentos diferentes.
		return errFileSizeLimitExceeded
	}

	// 4. Buscar espacio libre en el mapa (manyBytes)
	startBlockID, found := sfDacV3.indexMaster.GetBytesOff(manyBytes)
	if !found {
		// No hay espacio en los segmentos actuales. Creamos uno nuevo dinámicamente.

		// 1. Añadimos un nuevo mapa de bits vacío a la memoria RAM
		newBuffer := MakeAlignedBlock(sfDacV3.indexMaster.opts.SizeIndexMaster)

		sfDacV3.indexMaster.globalSize = append(sfDacV3.indexMaster.globalSize, newBuffer)

		// 2. Calculamos el nuevo tamaño físico que debe tener el archivo
		newSegmentsCount := len(sfDacV3.indexMaster.globalSize)

		//expandimos al tamaño justo del segmento anterior + el tamaño del nuevo indexmaster
		newFileSize := walSumBuffersSize +
			(int64(newSegmentsCount-1) * sfDacV3.indexMaster.segmentPhysicalSize) +
			int64(sfDacV3.opts.SizeIndexMaster)

		// 3. Expandimos el archivo en el disco
		sfDacV3.ExpandSize(newFileSize)

		// 4. Relanzamos la búsqueda (ahora garantizado que habrá espacio)
		startBlockID, found = sfDacV3.indexMaster.GetBytesOff(manyBytes)
		if !found {
			return errServerNotSizeAvaible
		}
	}

	var initSegIndex int
	var initIdIndex int
	var lastIdIndex int
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

		lastIdIndex = idIndex
		lastAlignedOffset4K = alignedOffset4K
	}

	// Verificamos si el archivo necesita crecer para cubrir los datos del último byte reservado
	requiredSize := walSumBuffersSize +
		(int64(initSegIndex) * sfDacV3.indexMaster.segmentPhysicalSize) +
		int64(sfDacV3.opts.SizeIndexMaster) +
		(int64(lastIdIndex+1) * sfDacV3.indexMaster.idIndexPhysicalSizePerByte)

	sfDacV3.ExpandSize(requiredSize)

	//Buffer exacto de los bytes actualizado
	bufIndexMaster := sfDacV3.indexMaster.globalSize[initSegIndex][initAlignedOffset4K : lastAlignedOffset4K+4096]

	OffsetIndexMaster := walSumBuffersSize + (int64(initSegIndex) * sfDacV3.indexMaster.segmentPhysicalSize) + int64(initAlignedOffset4K)

	// 1. Dónde empieza el bloque físico entero de este segmento (Absoluto)
	segmentOffset := walSumBuffersSize + int64(initSegIndex)*sfDacV3.indexMaster.segmentPhysicalSize

	// 2. El tamaño del mapa que debemos "saltar" para llegar a los datos
	headerMapSize := int64(sfDacV3.indexMaster.opts.SizeIndexMaster)

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

		var idIndex uint32
		if isSearch {

			idIndex = newIndexSearch(sfDacV3, fisrtOffsetIndex, uint32(sizePagination))

			pool := sfDacV3.indexSearchPool
			pool <- idIndex

		} else {

			idIndex = newIndex(sfDacV3, fisrtOffsetIndex, uint32(sizePagination))

			pool := sfDacV3.indexPools[uint32(sizePagination)]
			pool <- idIndex
		}

	}

	err = sfDacV3.WriteIndexMaster(bufIndexMaster, OffsetIndexMaster)
	if err != nil {
		return err
	}

	return nil
}
