package dacV3

func initReadIndex(sfDacV3 *dacV3) {

	// 1. Listar los indices actuales
	totalBlocks := len(sfDacV3.indexMaster.globalSize) * sfDacV3.opts.sizeIndexMaster

	for i := 0; i < totalBlocks; i++ {

		occupied, baseOffset := sfDacV3.indexMaster.Get(i)

		if !occupied {
			continue // Si existe un indice pero no esta en el mapa de almacenamiento, ese indice no existe (datos huerfanos)
		}

		// Leer el primer indice para determinar su configuracion
		idIndex, sizePagination, hash, index, err := sfDacV3.initIndex(baseOffset)
		if err != nil {
			continue
		}

		processLoadedIndex(sfDacV3, idIndex, sizePagination, hash, index)

		sizeBlock := sfDacV3.indexMaster.sizeSubIndex[sizePagination].sizeBlock

		// Validar si el índice abarca múltiples bloques o si un bloque contiene múltiples índices
		if sizeBlock < sfDacV3.indexMaster.idIndexPhysicalSizePerByte {

			indicesCount := sfDacV3.indexMaster.idIndexPhysicalSizePerByte / sizeBlock

			// Como ya procesamos el primero en offset baseOffset, procesamos el resto
			for j := int64(1); j < indicesCount; j++ {

				offset := baseOffset + j*sizeBlock

				idIdx, szPag, h, idx, e := sfDacV3.initIndex(offset)
				if e == nil {
					processLoadedIndex(sfDacV3, idIdx, szPag, h, idx)
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

	needed := int(sfDacV3.opts.NChanAvaibleIndexSearch) - len(sfDacV3.indexSearchPool)
	if needed > 0 {

		ids, err := sfDacV3.newIndexs(int64(needed), int64(sfDacV3.indexMaster.blockMaxSize.pageSize), true)
		if err == nil {

			for _, id := range ids {

				index := sfDacV3.indexLocation.Get(id)

				buf := sfDacV3.indexBuffer.getBufferArena(index.idLocationBuffer)

				bufIndex := indexBuffer(buf)

				hash := bufIndex.GetHashSearch()

				sfDacV3.indexSearch[hash] = IndexSearch{
					offset:           index.offset,
					idLocationBuffer: index.idLocationBuffer,
				}

				select {
				case sfDacV3.indexSearchPool <- id:
				default:
				}
			}
		}
	}

	// 2. Si no existen indices crear segun los valores
	for _, item := range sfDacV3.opts.SupportedSizes {

		pool := sfDacV3.indexPools[item.Size]

		// Usar los valores en IndexSizeChan (amplificado * 10)
		needed := int(item.IndexSizeChan) - len(pool)

		if needed > 0 {

			ids, err := sfDacV3.newIndexs(int64(needed), int64(item.Size), false)
			if err == nil {

				// En caso de que newIndexs devuelva indices de mas, usamos select default
				for _, id := range ids {

					select {

					case pool <- id:

					default:
					}
				}
			}
		}
	}
}

func processLoadedIndex(sfDacV3 *dacV3, idIndex uint32, sizePagination uint32, hash [32]byte, index *Index) {

	var emptyHash [32]byte
	if hash != emptyHash {

		// Y si tienen hash hay que meterlo en indexSearchPool y ademas añadirlo a indexSearch
		sfDacV3.indexSearch[hash] = IndexSearch{
			offset:           index.offset,
			idLocationBuffer: index.idLocationBuffer,
		}

		select {
		case sfDacV3.indexSearchPool <- idIndex:
		default:
		}

	} else {

		// Hay que llenar indexPools con los indices libres
		if pool, ok := sfDacV3.indexPools[sizePagination]; ok {
			select {
			case pool <- idIndex:
			default:
			}
		}
	}
}

func startHandleIndex(sfDacV3 *dacV3) {

	//Inicio donde se guardan los indices
	sfDacV3.indexLocation = NewPoolArray[Index]()

	//Donde se guardan los indices indexados a memoria
	sfDacV3.indexSearch = make(map[[32]byte]IndexSearch)

	sfDacV3.indexSearchPool = make(chan uint32, sfDacV3.opts.NChanAvaibleIndexSearch)

	//Incicio de los buffers para los indices
	sfDacV3.indexBuffer = newBufferArena(sfDacV3.opts.NBuffersAvailableIndex, BufferAlignSize)

	sfDacV3.indexPools = make(map[uint32]chan uint32)
	//Inicio de los canales donde se van a guardar los indices
	for _, item := range sfDacV3.opts.SupportedSizes {
		sfDacV3.indexPools[item.Size] = make(chan uint32, item.IndexSizeChan*10)
	}

	initReadIndex(sfDacV3)
}
