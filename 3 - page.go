package dacV3

import (
	"errors"
	"sync"
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

// Inicia todas las paginas de un indice, añadie su id y posicion al mapa de datos pero no el buffer
// Si el indice es indexSearch no se inicia
func (sfDacV3 *dacV3) InitAllPagesPerIndex(idIndex uint32, index *Index) {

	buf := sfDacV3.indexBuffer.getBufferArena(index.idLocationBuffer)
	if buf == nil {
		return
	}

	bufIndex := indexBuffer(buf)

	// Si el indice es indexSearch no se inicia
	var emptyHash [32]byte
	if bufIndex.GetHashSearch() != emptyHash {
		return
	}

	var needsUpdate bool

	// Comprobar lista de activados con todos los subindices de paginas
	for i := 0; i < MaxSubIndexPerIndex; i++ {

		if !bufIndex.IsIndexKept(i) {
			continue
		}

		// si en la lista se activo pero el subindice esta vacio borrarlo de la lista de activados
		size := bufIndex.GetSubIndexSize(i)
		if size == 0 {

			bufIndex.unSetSubIndex(i)
			needsUpdate = true
			continue
		}

		hash := bufIndex.GetSubIndexHash(i)

		seq := bufIndex.GetSubIndexSequence(i)

		sfDacV3.muPages.RLock()
		idPage, exists := sfDacV3.pages[hash]
		sfDacV3.muPages.RUnlock()
		if exists {

			existingPage := sfDacV3.pageLocation.Get(uint32(idPage))

			existingIndex := sfDacV3.indexLocation.Get(existingPage.idIndex)

			existingBuf := sfDacV3.indexBuffer.getBufferArena(existingIndex.idLocationBuffer)
			if existingBuf != nil {

				existingBufIndex := indexBuffer(existingBuf)

				existingSeq := existingBufIndex.GetSubIndexSequence(int(existingPage.idSubIndex))

				if seq > existingSeq {

					existingPage.mu.Lock()
					existingPage.idIndex = idIndex
					existingPage.idSubIndex = uint8(i)
					existingPage.mu.Unlock()

				}

			}

		} else {

			newIdPage, newPage := sfDacV3.pageLocation.New()

			newPage.idIndex = idIndex

			newPage.idSubIndex = uint8(i)

			sfDacV3.muPages.Lock()
			sfDacV3.pages[hash] = int(newIdPage)
			sfDacV3.muPages.Unlock()
		}
	}

	if needsUpdate {

		sfDacV3.updateIndex(index)

	}
}

var errPagedNotFound error = errors.New("page not found")

func (sfDacV3 *dacV3) LoadPage(hash [32]byte) (data []byte, err error) {

	sfDacV3.muPages.RLock()
	idPage, exists := sfDacV3.pages[hash]
	sfDacV3.muPages.RUnlock()

	if !exists {
		return nil, errPagedNotFound
	}

	page := sfDacV3.pageLocation.Get(uint32(idPage))

	page.mu.Lock()
	defer page.mu.Unlock()

	index := sfDacV3.indexLocation.Get(page.idIndex)

	bufIndex := indexBuffer(sfDacV3.indexBuffer.getBufferArena(index.idLocationBuffer))

	sizePagination := bufIndex.GetSizePagination()

	arena := sfDacV3.dataPools[int(sizePagination)]
	if arena == nil {
		return nil, errors.New("data pool no encontrado")
	}

	// Si ya está cargada en memoria
	if page.idBuffer != 0 {
		return arena.getBufferArena(page.idBuffer), nil
	}

	// Si no está cargada, la cargamos desde el disco
	idBuffer, buf := arena.addBufferArena()
	page.idBuffer = idBuffer

	// Calculamos el offset absoluto de la pagina
	pageOffset := sfDacV3.getOffsetPageStart(index, page)

	sfDacV3.ReadAt(buf, pageOffset)

	return buf, nil
}

func (sfDacV3 *dacV3) WritePage(hash [32]byte, data []byte, offset int64) {

}
//Si no existe error
func (sfDacV3 *dacV3) WriteIfExistPage(hash [32]byte, data []byte, offset int64) {

}