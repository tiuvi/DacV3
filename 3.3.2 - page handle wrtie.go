package dacV3

import (
	"bytes"
)

func (sfDacV3 *DacV3) CheckIndexPageFromHash(hash [32]byte) (err error) {

	// Llamamos a newPageHandle con create = false.
	// Él ya se encarga de verificar si existe y de devolvernos errPagedNotFound si no.
	sfIndexHandle, sfPageHandle, err := sfDacV3.newPageHandle(hash, []byte{}, 0, false)
	if err != nil {
		return err // Si no existe, devolverá tu errPagedNotFound
	}

	sfDacV3.CheckIndexPage(sfIndexHandle, sfPageHandle)

	return
}

func (sfDacV3 *DacV3) CheckIndexPage(sfIndexHandle *indexHandle, sfPageHandle *pageHandle) {

	println("\n chekeando la pagina")
	println("idIndex: ", sfPageHandle.idIndex)
	println("idSubIndex: ", sfPageHandle.idSubIndex)

	println("GetSubIndexSize: ", sfIndexHandle.Buf.GetSubIndexSize(int(sfPageHandle.idSubIndex)))
	println("GetSubIndexSequence: ", sfIndexHandle.Buf.GetSubIndexSequence(int(sfPageHandle.idSubIndex)))
	println("IsIndexKept: ", sfIndexHandle.Buf.IsIndexKept(int(sfPageHandle.idSubIndex)))

	hashArray := sfIndexHandle.Buf.GetSubIndexHash(int(sfPageHandle.idSubIndex))
	// 2. Ya le puedes hacer el slice [:] y convertirlo a string
	println("hash page: ", UUIDToString(hashArray))

	println("index SizePagination: ", sfIndexHandle.Buf.GetSizePagination())
	println("index hash: ", UUIDToString(sfIndexHandle.Buf.GetHashSearch()))
	println("Index checksum ", sfIndexHandle.Buf.GetCheckSum())
	println("Sequence: ", sfIndexHandle.Buf.GetSequence())

	trimmed := bytes.TrimRight(sfPageHandle.Buf.buf, "\x00")

	// 2. Definimos cuánto miden 3 líneas (3 x 64 bytes = 192 bytes)
	bytesUltimasLineas := 3 * 64

	var ultimas3Lineas string

	// 3. Comprobamos que haya al menos 192 bytes escritos
	if len(trimmed) >= bytesUltimasLineas {
		// Tomamos desde (LongitudTotal - 192) hasta el Final
		ultimas3Lineas = string(trimmed[len(trimmed)-bytesUltimasLineas:])
	} else {
		// Si la página tiene menos de 3 líneas, tomamos todo lo que haya
		ultimas3Lineas = string(trimmed)
	}

	println("Buffer Actual: \n", ultimas3Lineas)

	//println("buffer completo: \n", string(sfPageHandle.Buf.buf))
}


 
func (sfDacV3 *DacV3) writePageData(hash [32]byte, sfIndex *indexHandle, sfPageHandle *pageHandle, data []byte, offset int64) error {

	dataLen := int64(len(data))
	// Tamaño de los datos a escribir
	dataEnd := offset + dataLen

	//Si el tamaño supera el tamaño maximo de bloque.
	if dataEnd > sfDacV3.indexMaster.blockMaxSize.pageSize {

		//la clave para saber esto es si el tamaño es menor que el tamaño maximo, todavai no se ha configurado
		//la pagina de indices si el tamaño es mayor ya se configuro
		//sfIndex.GetSubIndexSize(int(sfPageHandle.idIndex))
		println("no configurado")
	}

	if dataEnd > int64(sfIndex.Buf.GetSizePagination()) {

		return sfDacV3.writePageDataSwapIndex(sfIndex, sfPageHandle, hash, data,dataLen, offset)
	}

	// Obtenemos el tamaño actual del archivo (filelen) de este subíndice
	fileLen := sfIndex.Buf.GetSubIndexSize(int(sfPageHandle.idSubIndex))

	// Calculamos el offset absoluto donde comienzan los datos de esta página en disco
	pageStartOffset := sfDacV3.getOffsetPageStart(sfIndex.Index, sfPageHandle.Page)

	// 1. Calculamos las fronteras alineadas a 4K relativas al buffer
	// Redondeo hacia abajo al múltiplo de 4096 más cercano
	start4K := offset &^ BufferAlignMask

	// Redondeo hacia arriba al múltiplo de 4096 más cercano
	end4K := (dataEnd + BufferAlignMask) &^ BufferAlignMask

	// 2. Calculamos el offset absoluto alineado a 4K para enviar a disco
	absoluteAlignedOffset := pageStartOffset + start4K

	// 3. Escribimos en el buffer en memoria
	sfPageHandle.Buf.mu.Lock()

	// IMPORTANTE: CopyAt devuelve un error (ej. Overflow), debemos capturarlo
	err := sfPageHandle.Buf.CopyAt(offset, data)
	if err != nil {
		sfPageHandle.Buf.mu.Unlock()
		return err
	}

	// Prevenimos un panic si end4K supera la capacidad real del buffer asignado en el Pool
	if end4K > int64(len(sfPageHandle.Buf.buf)) {
		end4K = int64(len(sfPageHandle.Buf.buf))
	}

	// 4. CREAMOS LA VISTA (Slice) ALINEADA A 4K
	// Esto no copia memoria, solo crea un slice que apunta a las páginas afectadas
	alignedDataView := sfPageHandle.Buf.buf[start4K:end4K]

	sfPageHandle.Buf.mu.Unlock()

	if start4K >= fileLen {

		return sfDacV3.writePageDirectData(sfIndex, dataEnd, fileLen, sfPageHandle, hash, offset, dataLen, alignedDataView, absoluteAlignedOffset)
	}

	//println("writePageData", gettLastLine(string(data)) , " offset: ", offset)
	//println("writePageData", gettLastLine(string(alignedDataView)) , " offset: ", offset)

	return sfDacV3.writePageWalData(sfIndex, dataEnd, fileLen, sfPageHandle, hash, offset, dataLen, alignedDataView, absoluteAlignedOffset)

}
