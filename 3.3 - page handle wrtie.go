package dacV3

import (
	"sync/atomic"
)

var TestCrashEnergy int64

const (
	CrashBeforeDiskWrite = 2
	CrashAfterDiskWrite  = 3
	CrashDuringIndexSwap = 4
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

	println("GetSubIndexSize: ", sfIndexHandle.GetSubIndexSize(int(sfPageHandle.idSubIndex)))
	println("GetSubIndexSequence: ", sfIndexHandle.GetSubIndexSequence(int(sfPageHandle.idSubIndex)))
	println("IsIndexKept: ", sfIndexHandle.IsIndexKept(int(sfPageHandle.idSubIndex)))

	hashArray := sfIndexHandle.GetSubIndexHash(int(sfPageHandle.idSubIndex))
	// 2. Ya le puedes hacer el slice [:] y convertirlo a string
	println("hash: ", UUIDToString(hashArray))

	println("index SizePagination: ", sfIndexHandle.GetSizePagination())
	println("Index checksum ", sfIndexHandle.GetCheckSum())
	println("Sequence: ", sfIndexHandle.GetSequence())

	println("Buffer: \n", string(sfPageHandle.Buf.buf))
	return
}

func (sfDacV3 *DacV3) writePageDataSwapIndex(sfIndexOld *indexHandle, sfPageHandle *pageHandle, data []byte, offset int64) error {

	sfDacV3.CheckIndexPage(sfIndexOld, sfPageHandle)

	dataEnd := offset + int64(len(data))

	sfPageHandle.mu.Lock()
	//Bloqueamos buffer antiguo para hacer cambio de buffer
	oldBuf := sfPageHandle.Buf
	oldBuf.mu.Lock()

	//Primeramente obtenemos un indice nuevo reservado, solo en memoria
	sfIndexNew, newIdIndex, newIdSubIndex, err := sfDacV3.UpdatePageInIndex(sfPageHandle.idSubIndex, uint32(dataEnd))
	if err != nil {
		oldBuf.mu.Unlock()
		sfPageHandle.mu.Unlock()
		return err
	}

	oldIndexSubIndex := sfPageHandle.idSubIndex
	// ¡CRÍTICO! El sub-índice DEBE asignarse ANTES del atomic.Store.
	sfPageHandle.idSubIndex = newIdSubIndex

	//Asignamos el nuevo indice a la pagina
	atomic.StoreUint32(&sfPageHandle.idIndex, newIdIndex)

	sizePagination := sfIndexNew.GetSizePagination()

	arena := sfDacV3.dataPools[int(sizePagination)]

	newIdBuffer, newBuf := arena.New()

	//Copiamos antiguo buffer al nuevo
	newBuf.CopyAt(0, oldBuf.buf)

	//Escribimos datos en el NUEVO buffer
	newBuf.CopyAt(offset, data)

	sfPageHandle.Buf = newBuf

	oldIdBuffer := atomic.LoadUint32(&sfPageHandle.idBuffer)

	atomic.StoreUint32(&sfPageHandle.idBuffer, newIdBuffer)

	oldBuf.mu.Unlock()

	// Liberamos el buffer antiguo
	oldArena := sfDacV3.dataPools[int(sfIndexOld.GetSizePagination())]

	oldArena.Free(oldIdBuffer)

	// Estoy hay que hacerlo con el nuevo indice
	pageStartOffset := sfDacV3.getOffsetPageStart(sfIndexNew.Index, sfPageHandle.Page)

	if TestCrashEnergy == CrashBeforeDiskWrite {
		panic("SIMULANDO CORTE DE ENERGÍA 🔌💥 CrashBeforeDiskWrite")
	}

	err = sfDacV3.WritePageDirect(newBuf.buf, pageStartOffset, func() {

		sfPageHandle.mu.Unlock()
	})
	if err != nil {
		return err
	}

	sfDacV3.CheckIndexPage(sfIndexNew, sfPageHandle)

	if TestCrashEnergy == CrashAfterDiskWrite {
		panic("SIMULANDO CORTE DE ENERGÍA 🔌💥 CrashAfterDiskWrite")
	}

	sfDacV3.SwapIndexDirection(sfIndexOld, oldIndexSubIndex, sfIndexNew, newIdSubIndex, dataEnd)

	println("SE termina de ejecutar la ampliacion")
	return nil
}

func (sfDacV3 *DacV3) writePageData(sfIndex *indexHandle, sfPageHandle *pageHandle, data []byte, offset int64) error {

	// Tamaño de los datos a escribir
	dataEnd := offset + int64(len(data))

	if dataEnd > int64(sfIndex.GetSizePagination()) {

		println("El archivo supera: ", sfIndex.GetSizePagination())

		return sfDacV3.writePageDataSwapIndex(sfIndex, sfPageHandle, data, offset)
	}

	// Obtenemos el tamaño actual del archivo (filelen) de este subíndice
	fileLen := sfIndex.GetSubIndexSize(int(sfPageHandle.idSubIndex))

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

	// 5. Decidimos qué método de escritura en disco usar
	if start4K >= fileLen {

		// ESCRITURA DIRECTA (Append)
		sfPageHandle.mu.Lock()

		// Enviamos el slice alineado a 4K y el offset absoluto alineado a 4K
		err = sfDacV3.WritePageDirect(alignedDataView, absoluteAlignedOffset, func() {
			sfPageHandle.mu.Unlock()
		})
		if err != nil {
			return err
		}

	} else {

		// ESCRITURA WALL (Update/Sobrescritura)
		sfPageHandle.mu.Lock()

		// Enviamos el slice alineado a 4K y el offset absoluto alineado a 4K
		err = sfDacV3.WritePageWall(alignedDataView, absoluteAlignedOffset, func() {
			sfPageHandle.mu.Unlock()
		})
		if err != nil {
			return err
		}
	}

	// 6. Actualizar el filelen si los datos escritos extienden el tamaño
	if dataEnd > fileLen {
		sfIndex.mu.Lock()
		sfIndex.SetSubIndexSize(int(sfPageHandle.idSubIndex), dataEnd)
		sfDacV3.updateIndex(sfIndex.Index)
		sfIndex.mu.Unlock()
	}

	return nil
}
