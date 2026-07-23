package dacV3

import (
	"log"
	"sync/atomic"
)

func initReadIndex(sfDacV3 *DacV3) {

	// 1. Listar los indices actuales
	totalBlocks := len(sfDacV3.indexMaster.globalSize) * sfDacV3.opts.SizeIndexMaster

	for i := 0; i < totalBlocks; i++ {

		occupied, baseOffset := sfDacV3.indexMaster.Get(i)

		if !occupied {
			continue // Si existe un indice pero no esta en el mapa de almacenamiento, ese indice no existe (datos huerfanos)
		}

		// Leer el primer indice para determinar su configuracion
		idIndex, sizePagination, hash, slotsFree, index, err := sfDacV3.initIndex(baseOffset)
		if err != nil {
			continue
		}

		processLoadedIndex(sfDacV3, idIndex, sizePagination, hash, slotsFree, index)

		sizeBlock := sfDacV3.indexMaster.sizeSubIndex[sizePagination].sizeBlock

		// Validar si el índice abarca múltiples bloques o si un bloque contiene múltiples índices
		if sizeBlock < sfDacV3.indexMaster.idIndexPhysicalSizePerByte {

			indicesCount := sfDacV3.indexMaster.idIndexPhysicalSizePerByte / sizeBlock

			// Como ya procesamos el primero en offset baseOffset, procesamos el resto
			for j := int64(1); j < indicesCount; j++ {

				offset := baseOffset + j*sizeBlock

				idIdx, szPag, h, slotsFree, idx, e := sfDacV3.initIndex(offset)
				if e == nil {
					processLoadedIndex(sfDacV3, idIdx, szPag, h, slotsFree, idx)
				}

			}

		} else {

			// Un indice abarca multiples bytes en el mapa
			bytesCount := sizeBlock / sfDacV3.indexMaster.idIndexPhysicalSizePerByte

			if bytesCount > 1 {
				i += int(bytesCount) - 1 // Saltar los bytes adicionales de este mismo indice
			}
		}
	}

	needed := sfDacV3.needIndexs(int64(sfDacV3.indexMaster.blockMaxSize.pageSize), true)
	if needed > 0 {

		err := sfDacV3.newIndexs(int64(needed), int64(sfDacV3.indexMaster.blockMaxSize.pageSize), true)
		if err != nil {
			log.Fatalln(err.Error())
		}
	}

	// 2. Si no existen indices crear segun los valores
	for _, item := range sfDacV3.opts.SupportedSizes {

		needed = sfDacV3.needIndexs(int64(item.Size), false)
		if needed > 0 {

			err := sfDacV3.newIndexs(int64(needed), int64(item.Size), false)
			if err != nil {
				log.Fatalln(err.Error())
			}
		}
	}
}

func processLoadedIndex(sfDacV3 *DacV3, idIndex uint32, sizePagination uint32, hash [32]byte, slotsFree int64, index *Index) {

	var emptyHash [32]byte
	if hash != emptyHash {

		// Y si tienen hash hay que meterlo en indexSearchPool y ademas añadirlo a indexSearch
		sfDacV3.indexSearch[hash] = IndexSearch{
			offset:           index.offset,
			idLocationBuffer: index.idLocationBuffer,
		}

		sfDacV3.indexAvailableSlotsSearch.Add(slotsFree)

		select {
		case sfDacV3.indexSearchPool <- IndexPoolItem{
			IDIndex:        idIndex,
			AvailableSlots: uint8(slotsFree),
		}:
		default:
		}

	} else {

		sfDacV3.indexAvailableSlots[sizePagination].Add(slotsFree)

		// Hay que llenar indexPools con los indices libres
		if pool, ok := sfDacV3.indexPools[sizePagination]; ok {
			select {
			case pool <- IndexPoolItem{
				IDIndex:        idIndex,
				AvailableSlots: uint8(slotsFree),
			}:
			default:
			}
		}
	}
}

func startHandleIndex(sfDacV3 *DacV3) {

	//Inicio donde se guardan los indices
	sfDacV3.indexLocation = NewPoolArray[Index](1000)

	//Donde se guardan los indices indexados a memoria
	sfDacV3.indexSearch = make(map[[32]byte]IndexSearch)

	sfDacV3.indexSearchPool = make(chan IndexPoolItem, sfDacV3.opts.NChanAvaibleIndexSearch)

	sfDacV3.indexAvailableSlotsSearch = new(atomic.Int64)

	//Incicio de los buffers para los indices
	sfDacV3.indexBuffer = newBufferArena(sfDacV3.opts.NBuffersAvailableIndex, BufferAlignSize)

	sfDacV3.indexPools = make(map[uint32]chan IndexPoolItem)

	sfDacV3.indexAvailableSlots = make(map[uint32]*atomic.Int64)

	//Inicio de los canales donde se van a guardar los indices
	for _, item := range sfDacV3.opts.SupportedSizes {

		sfDacV3.indexPools[item.Size] = make(chan IndexPoolItem, item.IndexSizeChan*10)

		sfDacV3.indexAvailableSlots[item.Size] = new(atomic.Int64)
	}

	initReadIndex(sfDacV3)
}
