package dacV3

const BufferAlignSize = 4096 // Tamaño de bloque típico en Linux (ext4/XFS)

// 4 bloques fijos de 4096 perdidos para índices
const IndexOverheadBlocks int64 = 4

type configIndex struct {
	//Total de bloques 4096
	blocks int64
	//Tamaño total del bloque
	sizeBlock int64
	//Total de paginas que entran por bloque
	pages int64
	//Tamaño total de la pagina
	pageSize int64
	//offset donde comienzan las paginas
	pageStartOffset int64
}

// init se ejecuta automáticamente al iniciar el programa, antes que main()
func startHandleConfigIndexSize(sfDacV3 *dacV3) {

	sfDacV3.globalSizeSubIndex = make(map[uint32]configIndex)

	// Iteramos desde 1 hasta 16 para abarcar desde 4096 hasta 65536
	for multiplier := int64(1); multiplier <= 32; multiplier++ {

		pageSize := multiplier * BufferAlignSize

		blocks := multiplier * MaxSubIndexPerIndex

		// Aseguramos que haya suficientes bloques para descontar la cabecera
		var pages int64 = 0
		if blocks > IndexOverheadBlocks {
			pages = (blocks - IndexOverheadBlocks) / multiplier
		}

		// CORRECCIÓN: Tamaño total del bloque - (Cantidad de páginas * Tamaño de cada página)
		// De esta forma, garantizamos que los datos terminan exactamente en el último byte.
		startOffset := (blocks * BufferAlignSize) - (pages * pageSize)

		sfDacV3.globalSizeSubIndex[uint32(pageSize)] = configIndex{
			blocks:          blocks,
			sizeBlock:       blocks * BufferAlignSize,
			pages:           pages,
			pageSize:        pageSize,
			pageStartOffset: startOffset,
		}
	}



	
}
