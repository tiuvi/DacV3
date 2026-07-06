package main

import (
	"errors"
	"sync"
)

const minSizeIndexWithPage = 4096*maxSubIndexPerIndex /* subindices * bloques */ + 4096 //indice

const (

	// Checksum
	field_IndexCheckSumInit = 0
	field_IndexCheckSumEnd  = 4

	// Secuencia
	field_IndexSequenceInit = 4
	field_IndexSequenceEnd  = 12

	// Tamaño de paginación
	field_IndexSizePaginationInit = 12
	field_IndexSizePaginationEnd  = 16

	// numero de elementos del subíndice
	field_IndexLenSubIndexInit = 16
	field_IndexLenSubIndexEnd  = 20

	// Subíndices activos / Kept
	field_IndexKeptInit = 20
	field_IndexKeptEnd  = 20 + maxSubIndexPerIndex

	field_HashSearchInit = field_IndexKeptEnd
	field_HashSearchEnd  = field_HashSearchInit + 32
)


// subindices tamaño
const (

	// Posiciones relativas dentro de un único subíndice
	subIndex_Hash_Init = 0
	subIndex_Hash_End  = 32

	subIndex_Size_Init = 32
	subIndex_Size_End  = 40
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

//Calculo del indice + el numero de subindices por el tamaño de cada indice
const totalIndexAndSubIndex = BufferAlignSize - sizeTotalIndex

//Dejamos padding para los indices empezando los subindex posterior.
const field_subIndexInit = BufferAlignSize - sizeSubIndexInBlock



// Indice
type Index struct {
	//Bloqueo de mutex
	mu sync.Mutex
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
