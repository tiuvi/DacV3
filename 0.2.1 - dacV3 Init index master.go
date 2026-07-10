package dacV3

import (
	"log"
)

const (
	Kilobyte = 1024
	Megabyte = Kilobyte * 1024
	Gigabyte = Megabyte * 1024
	Terabyte = Gigabyte * 1024
)

//TOTAL DE BLOQUES QUE PUEDE ALMACENAR EL ARCHIVO 12 gb
//const totalIndexBlocks = ((sizeIndexMaster * 8) * (98 * BufferAlignSize)) / Gigabyte

func calcIndexMasterStaticSize(sfDacV3 *dacV3) {

	// 1. PRIMERO: Calcular las matemáticas (Obligatorio aunque el archivo sea nuevo)
	sfDacV3.indexMaster.idIndexPhysicalSizePerByte = sfDacV3.indexMaster.blockMinSize.sizeBlock * 8

	dataPhysicalSize := sfDacV3.indexMaster.idIndexPhysicalSizePerByte * int64(sfDacV3.opts.sizeIndexMaster)

	sfDacV3.indexMaster.segmentPhysicalSize = int64(sfDacV3.opts.sizeIndexMaster) + dataPhysicalSize

	return
}

func initIndexMasterBuffers(sfDacV3 *dacV3) (needReadDisk bool) {

	walSumBuffersSize := sfDacV3.dacV3WorkerWriter.walSumBuffersSize

	fileSize := sfDacV3.len.Load()
	if fileSize == 0 {
		// Ahora sfDacV3.indexMaster.segmentPhysicalSize SÍ tiene un valor mayor a 0
		sfDacV3.ExpandSize(walSumBuffersSize + sfDacV3.indexMaster.segmentPhysicalSize)

		newBuffer := MakeAlignedBlock(sfDacV3.opts.sizeIndexMaster)

		sfDacV3.indexMaster.globalSize = [][]byte{newBuffer}
		return false
	}

	// 4. Calculamos cuántos segmentos (mapas de bits) hay en el archivo actual.
	// Si el archivo es nuevo (0 bytes), de todas formas necesitamos inicializar el primer mapa (índice 0).
	dataSize := fileSize - walSumBuffersSize

	// 4. Calculamos cuántos segmentos (mapas de bits) hay en el archivo actual.
	numSegments := int(dataSize / sfDacV3.indexMaster.segmentPhysicalSize)
	if dataSize%sfDacV3.indexMaster.segmentPhysicalSize != 0 {
		numSegments++
	}

	// 5. Inicializamos el slice multidimensional para albergar exactamente los mapas necesarios
	sfDacV3.indexMaster.globalSize = make([][]byte, numSegments)

	return true
}

func readIndexMaster(sfDacV3 *dacV3) {

	walSumBuffersSize := sfDacV3.dacV3WorkerWriter.walSumBuffersSize

	// 6. Leemos cada mapa de bits desde el disco
	for i := 0; i < len(sfDacV3.indexMaster.globalSize); i++ {

		//este seria el primer bloque de indice
		physicalOffset := walSumBuffersSize + (int64(i) * sfDacV3.indexMaster.segmentPhysicalSize)

		// Asignamos memoria alineada para leer el bloque con O_DIRECT
		newBuffer := MakeAlignedBlock(sfDacV3.opts.sizeIndexMaster)

		// Leemos directamente del disco al buffer alineado
		sfDacV3.ReadAt(newBuffer, physicalOffset)

		// Almacenamos el bloque en nuestra estructura en memoria RAM
		sfDacV3.indexMaster.globalSize[i] = newBuffer
	}

	return
}

func startHandleIndexMaster(sfDacV3 *dacV3) {

	indexMaster := indexMaster{}

	sfDacV3.indexMaster = &indexMaster

	sfDacV3.indexMaster.opts = sfDacV3.opts

	sfDacV3.indexMaster.sizeSubIndex = make(map[uint32]configIndex)

	sfDacV3.indexMaster.blockMinSize = configIndex{}
	sfDacV3.indexMaster.blockMaxSize = configIndex{}

	for _, item := range sfDacV3.opts.SupportedSizes {

		data, found := globalSizeSubIndex[item.Size]
		if !found {
			log.Fatal("Tamaño de indice no compatible")
		}

		sfDacV3.indexMaster.sizeSubIndex[item.Size] = data

		//Iniciamos el estado del bloque minimo con el primer tamaño compatible
		if sfDacV3.indexMaster.blockMinSize.pageSize == 0 {
			sfDacV3.indexMaster.blockMinSize = data
			sfDacV3.indexMaster.blockMaxSize = data
		}

		//Si la siguiente interaccion el tamaño del bloque minimo es mayor lo cambiamos.
		if sfDacV3.indexMaster.blockMinSize.pageSize > int64(data.pageSize) {
			sfDacV3.indexMaster.blockMinSize = data
		}

		//Si la siguiente interaccion el tamaño del bloque maximo es mayor lo cambiamos.
		if sfDacV3.indexMaster.blockMaxSize.pageSize < int64(data.pageSize) {
			sfDacV3.indexMaster.blockMaxSize = data
		}
	}

	calcIndexMasterStaticSize(sfDacV3)

	if initIndexMasterBuffers(sfDacV3) {
		readIndexMaster(sfDacV3)
	}

}
