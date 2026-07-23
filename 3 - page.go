package dacV3

import (
	"errors"
	"sync"
	"sync/atomic"
)

var (
	errPagedNotFound  error = errors.New("page not found")
	ErrNegativeOffset       = errors.New("offset cannot be negative")
	ErrBufferOverflow       = errors.New("data exceeds buffer capacity")
)

// Datos
type Page struct {
	mu sync.Mutex // Añadimos el mutex para proteger el acceso concurrente
	//Indice del buffer sincronizado
	idBuffer uint32
	//Indice del inice al que pertenece la pagina
	idIndex uint32
	//Indice del subindice al que pertenece la pagina
	idSubIndex uint8
}

type pageHandle struct {
	*Page
	Buf *GlobalBuffer
}

func (sfDacV3 *DacV3) newPageHandleCreate(hash [32]byte, data []byte, offset int64) (sfIndexHandle *indexHandle, sfPageHandle *pageHandle, err error) {

	//Creacion unica de pageLocation con bloqueo minimo
	idPage := sfDacV3.pages.StoreOrGet(hash, func() uint32 {

		newIdPage, _ := sfDacV3.pageLocation.New()

		return newIdPage
	})

	//Ya hemos creado la pagina, ahora hay que asignarle un indice
	sfPage := sfDacV3.pageLocation.Get(uint32(idPage))

	//Llegados a este punto bloqueamos para la creacion de indice y de buffer conjuntas
	//Y preparamos para que todas las peticiones en vuelo que se queden en este mutex
	//Cuando se debloque no repitan la misma accion
	sfPage.mu.Lock()
	defer sfPage.mu.Unlock()

	//Primer caso bajo lock de pagina, cuando no esta asignado ningun indice
	//Despues de la insercion en el mapa no sabemos si somos los primeros en obtener el candado
	var newIdIndex uint32
	var newIdSubIndex uint8
	if atomic.LoadUint32(&sfPage.idIndex) == 0 {

		// 1. Calculamos el espacio requerido
		requiredSpace := uint32(offset) + uint32(len(data))

		// 2. Creamos un handle "vacío" temporalmente (sin buffer aún)
		// Esto es necesario porque CreatePageInIndex necesita el sfPageHandle para inyectarle los IDs
		sfPageHandle = &pageHandle{
			Page: sfPage,
		}

		// 3. Le asignamos un espacio en el índice PRIMERO (Aquí se llena sfPageHandle.idIndex)
		sfIndexHandle, newIdIndex, newIdSubIndex, err = sfDacV3.CreatePageInIndex(hash, requiredSpace)
		if err != nil {
			return
		}

	} else {

		sfPageHandle = &pageHandle{
			Page: sfPage,
		}

		sfIndexHandle, err = sfDacV3.newIndexHandle(sfPageHandle.idIndex)
		if err != nil {
			return nil, nil, err
		}
	}

	var newIdBuffer uint32
	if atomic.LoadUint32(&sfPage.idBuffer) == 0 {

		// 4. AHORA que ya tenemos índice (y sabemos el tamaño), le asignamos un Buffer.
		// Como YA tenemos el lock de sfPage, no podemos usar newPageHandle. Lo hacemos directo:
		sizePagination := sfIndexHandle.GetSizePagination()

		arena := sfDacV3.dataPools[int(sizePagination)]

		var buf *GlobalBuffer
		newIdBuffer, buf = arena.New()

		sfPageHandle.Buf = buf

	} else {

		sizePagination := sfIndexHandle.GetSizePagination()

		arena := sfDacV3.dataPools[int(sizePagination)]

		sfPageHandle.Buf = arena.Get(atomic.LoadUint32(&sfPage.idBuffer))

	}

	if newIdBuffer != 0 {
		//IMPORTANTE ULTIMO PASO PARA EVITAR DATA RACE
		atomic.StoreUint32(&sfPage.idBuffer, newIdBuffer)
	}

	//IMPORTANTE ESTE ES UN PASO FINAL SI NO DATA RACE EN LECTURA DE BUFFERS EN newPageHandle
	if newIdIndex != 0 {
		// ¡CRÍTICO! El sub-índice DEBE asignarse ANTES del atomic.Store.
		// De lo contrario, un hilo en newPageHandle podría leer idIndex != 0
		// y proceder a leer un idSubIndex que aún no ha sido asignado.
		sfPage.idSubIndex = newIdSubIndex
		atomic.StoreUint32(&sfPage.idIndex, newIdIndex)
	}

	return sfIndexHandle, sfPageHandle, nil
}

func (sfDacV3 *DacV3) newPageHandle(hash [32]byte, data []byte, offset int64, create bool) (sfIndexHandle *indexHandle, sfPageHandle *pageHandle, err error) {

	idPage, exists := sfDacV3.pages.Get(hash)

	//Si no hay que crearlo y no existe la pagina no existe
	if !create && !exists {
		return nil, nil, errPagedNotFound
	}

	//Si hay que crearlo y no existe la pagina se crea
	if create && !exists {
		return sfDacV3.newPageHandleCreate(hash, data, offset)
	}

	//Si la pagina existe se carga
	page := sfDacV3.pageLocation.Get(idPage)

	//Si existe puede ser que tenga indice o puede que no sin mutex usamos el numero de indice atomico para verificar
	//En ese caso enviamos a la espera del mutex en newpagehandlecreate
	if atomic.LoadUint32(&page.idIndex) == 0 {
		return sfDacV3.newPageHandleCreate(hash, data, offset)
	}

	sfIndexHandle, err = sfDacV3.newIndexHandle(page.idIndex)
	if err != nil {
		return
	}

	sizePagination := sfIndexHandle.GetSizePagination()

	realSizePage := sfIndexHandle.GetSubIndexSize(int(page.idSubIndex))

	arena := sfDacV3.dataPools[int(sizePagination)]

	//En caso de tener indice puede ser que el buffer no este cargado lo verificamos tambien de manera atomica
	if atomic.LoadUint32(&page.idBuffer) == 0 {

		page.mu.Lock()
		defer page.mu.Unlock()

		// Doble comprobación (Double-checked locking): verificamos si otro hilo
		// inicializó el buffer mientras esperábamos el mutex.
		if atomic.LoadUint32(&page.idBuffer) != 0 {

			handleBuf := arena.Get(atomic.LoadUint32(&page.idBuffer))

			return sfIndexHandle, &pageHandle{
				Page: page,
				Buf:  handleBuf,
			}, nil
		}

		// Si llegamos aquí, NO está cargada. La cargamos desde el disco.
		newIdBuffer, handleBuf := arena.New()

		// Calculamos el offset absoluto de la pagina
		pageOffset := sfDacV3.getOffsetPageStart(sfIndexHandle.Index, page)

		if realSizePage != 0 {

			sfDacV3.ReadAt(handleBuf.buf, pageOffset)

			// Limpiamos los bytes que NO se han escrito (en lugar de borrar los válidos)
			clear(handleBuf.buf[realSizePage:])
		}

		// GUARDAMOS EL ID DE FORMA ATÓMICA, IMPORTANTE ULTIMO PASO
		atomic.StoreUint32(&page.idBuffer, newIdBuffer)

		return sfIndexHandle, &pageHandle{
			Page: page,
			Buf:  handleBuf,
		}, nil
	}

	handleBuf := arena.Get(atomic.LoadUint32(&page.idBuffer))

	return sfIndexHandle, &pageHandle{
		Page: page,
		Buf:  handleBuf, // Corregido: tú usabas GlobalBuffer: data
	}, nil

}

func (sfDacV3 *DacV3) WritePage(hash [32]byte, data []byte, offset int64) error {

	sfIndexHandle, sfPageHandle, err := sfDacV3.newPageHandle(hash, data, offset, true)
	if err != nil {
		return err // Si no existe, devolverá tu errPagedNotFound
	}

	err = sfDacV3.writePageData(sfIndexHandle, sfPageHandle, data, offset)
	if err != nil {
		return err
	}

	return nil
}

func (sfDacV3 *DacV3) WriteIfExistPage(hash [32]byte, data []byte, offset int64) error {
	// Llamamos a newPageHandle con create = false.
	// Él ya se encarga de verificar si existe y de devolvernos errPagedNotFound si no.
	sfIndexHandle, sfPageHandle, err := sfDacV3.newPageHandle(hash, data, offset, false)
	if err != nil {
		return err // Si no existe, devolverá tu errPagedNotFound
	}

	return sfDacV3.writePageData(sfIndexHandle, sfPageHandle, data, offset)
}

// writePageData escribe datos en una pagina existente
// Decide entre WritePageDirect (datos nuevos mas alla de filelen) o WritePageWall (sobreescritura dentro de filelen)

func (sfDacV3 *DacV3) writePageDataSwapIndex(sfIndex *indexHandle, sfPageHandle *pageHandle, data []byte, offset int64) error {

	dataEnd := offset + int64(len(data))

	sfPageHandle.mu.Lock()
	//Bloqueamos buffer antiguo para hacer cambio de buffer
	oldBuf := sfPageHandle.Buf
	oldBuf.mu.Lock()

	newIndex, newIdIndex, newIdSubIndex, err := sfDacV3.UpdatePageInIndex(sfIndex, sfPageHandle.idSubIndex, uint32(dataEnd))
	if err != nil {
		oldBuf.mu.Unlock()
		sfPageHandle.mu.Unlock()
		return err
	}

	// ¡CRÍTICO! El sub-índice DEBE asignarse ANTES del atomic.Store.
	sfPageHandle.idSubIndex = newIdSubIndex

	atomic.StoreUint32(&sfPageHandle.idIndex, newIdIndex)

	sizePagination := newIndex.GetSizePagination()

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

	sfPageHandle.mu.Unlock()

	// Liberamos el buffer antiguo
	oldArena := sfDacV3.dataPools[int(sfIndex.GetSizePagination())]
	oldArena.Free(oldIdBuffer)

	// Estoy hay que hacerlo con el nuevo indice
	pageStartOffset := sfDacV3.getOffsetPageStart(newIndex.Index, sfPageHandle.Page)

	sfPageHandle.mu.Lock()
	err = sfDacV3.WritePageDirect(data, pageStartOffset, func() {
		sfPageHandle.mu.Unlock()
	})
	if err != nil {
		return err
	}

	return nil
}

func (sfDacV3 *DacV3) writePageData(sfIndex *indexHandle, sfPageHandle *pageHandle, data []byte, offset int64) error {

	// Tamaño de los datos a escribir
	dataEnd := offset + int64(len(data))

	if dataEnd > int64(sfIndex.GetSizePagination()) {

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

func (sfDacV3 *DacV3) ReadPage(hash [32]byte, data []byte, offset int64) (n int, err error) {

	sfIndexHandle, sfPageHandle, err := sfDacV3.newPageHandle(hash, data, offset, true)
	if err != nil {
		return 0, err // Si no existe, devolverá tu errPagedNotFound
	}

	return sfDacV3.readPageData(sfIndexHandle, sfPageHandle, data, offset), nil
}

func (sfDacV3 *DacV3) ReadIfExistPage(hash [32]byte, data []byte, offset int64) (n int, err error) {
	// Llamamos a newPageHandle con create = false.
	// Él ya se encarga de verificar si existe y de devolvernos errPagedNotFound si no.
	sfIndexHandle, sfPageHandle, err := sfDacV3.newPageHandle(hash, data, offset, false)
	if err != nil {
		return 0, err // Si no existe, devolverá tu errPagedNotFound
	}

	return sfDacV3.readPageData(sfIndexHandle, sfPageHandle, data, offset), nil
}

func (sfDacV3 *DacV3) readPageData(sfIndex *indexHandle, sfPageHandle *pageHandle, data []byte, offset int64) (n int) {

	if offset < 0 {
		return 0
	}

	// 1. Obtenemos el tamaño real de los datos válidos en disco/memoria (filelen)
	fileLen := sfIndex.GetSubIndexSize(int(sfPageHandle.idSubIndex))

	// 2. Si el offset solicitado está más allá de lo que se ha escrito, no hay nada que leer
	if offset >= fileLen {
		return 0
	}

	// 3. Calculamos la longitud exacta a leer
	// Si nos piden leer más allá del tamaño real, cortamos la lectura hasta el fileLen
	readLen := int64(len(data))
	if offset+readLen > fileLen {
		readLen = fileLen - offset
	}

	// 4. Bloqueamos el buffer para evitar que otra gorutina lo modifique mientras leemos
	// NOTA: Si pb.mu es un sync.RWMutex, aquí deberías usar sfPageHandle.Buf.mu.RLock()
	sfPageHandle.Buf.mu.RLock()
	defer sfPageHandle.Buf.mu.RUnlock()

	// 6. Copiamos los datos desde la memoria de la página al slice del usuario
	copied := copy(data, sfPageHandle.Buf.buf[offset:offset+readLen])

	return copied
}
