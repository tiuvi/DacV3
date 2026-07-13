package dacV3

import (
	"errors"
	"sync"
)

// Indice principal
const (

	// Checksum
	field_IndexCheckSumInit = 0
	field_IndexCheckSumEnd  = field_IndexCheckSumInit + 4

	// Secuencia
	field_IndexSequenceInit = field_IndexCheckSumEnd
	field_IndexSequenceEnd  = field_IndexSequenceInit + 8

	// Tamaño de paginación
	field_IndexSizePaginationInit = field_IndexSequenceEnd
	field_IndexSizePaginationEnd  = field_IndexSizePaginationInit + 4

	// Subíndices activos / Kept
	field_IndexKeptInit = field_IndexSizePaginationEnd
	field_IndexKeptEnd  = field_IndexKeptInit + maxSubIndexPerIndex

	//Añadir este indice a la busqueda
	field_HashSearchInit = field_IndexKeptEnd
	field_HashSearchEnd  = field_HashSearchInit + 32
)

// maximo de subindices por indice 4096
const maxSubIndexPerIndex = 82

const freeSizeInIndex = BufferAlignSize - sizeTotalIndex

// subindices tamaño
const (

	// Posiciones relativas dentro de un único subíndice
	subIndex_Hash_Init = 0
	subIndex_Hash_End  = subIndex_Hash_Init + 32

	subIndexSequence_Init = subIndex_Hash_End
	subIndexSequence_End  = subIndexSequence_Init + 8

	subIndex_Size_Init = subIndexSequence_End
	subIndex_Size_End  = subIndex_Size_Init + 8
)

// Nuevo pagina para analiticas
const (
	subIndex_Name_Init = 0
	subIndex_Name_End  = 8 * 8

	subIndex_LastAccess_Init = subIndex_Name_End
	subIndex_LastAccess_End  = subIndex_Name_End + 8

	subIndex_LastUpdate_Init = subIndex_LastAccess_End
	subIndex_LastUpdate_End  = subIndex_LastAccess_End + 8
)

const indexMetricsTotalSize = subIndex_LastUpdate_End * maxSubIndexPerIndex

const sizeSubIndexMetric = BufferAlignSize / maxSubIndexPerIndex

const sizeSubIndex = subIndex_Size_End

const sizeSubIndexInBlock = sizeSubIndex * maxSubIndexPerIndex

// Sin usar sizeTotalIndex
const sizeTotalIndex = sizeSubIndexInBlock + field_HashSearchEnd

// Calculo del indice + el numero de subindices por el tamaño de cada indice
const totalIndexAndSubIndex = BufferAlignSize - sizeTotalIndex

// Dejamos padding para los indices empezando los subindex posterior.
const field_subIndexInit = BufferAlignSize - sizeSubIndexInBlock

// Indice en array necesita su offset para poder eliminar el buffer y volver abrir el buffer cuando se use
type Index struct {
	//Bloqueo de mutex
	mu sync.Mutex
	//Localizacion del indice en el sistema de archivos
	offset int64
	//Array donde esta el buffer
	idLocationBuffer uint32
}

/*
No necesita mutex ya que depende de otra pagina padre
*/
type IndexSearch struct {
	//Localizacion del indice en el sistema de archivos
	offset int64
	//Array donde esta el buffer
	idLocationBuffer uint32
}

type SubIndex struct {
	hash [32]byte
	size int64
	name string //maximo 128
}

// Funciones para manipular los indices y los subindices
type indexBuffer []byte

// Funiones para manipular las metricas de los indices
type indexBufferMetric []byte

var errSubIndexOverFlow = errors.New("tamaño maximo de subindices de pagina superado")
