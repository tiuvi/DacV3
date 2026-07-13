package dacV3

import (
	"os"
	"sync"
	"sync/atomic"
	"unsafe"
)

type indexMaster struct {
	globalSize [][]byte
	//Este bloque es el tamaño minimo por bloque
	blockMinSize               configIndex
	blockMaxSize               configIndex
	segmentPhysicalSize        int64
	idIndexPhysicalSizePerByte int64
	sizeSubIndex               map[uint32]configIndex
	opts                       *DacV3Options
	walSumBuffersSize          int64
	maxMinRelationBlock        int
}

type dacV3 struct {
	//Gestion del archivo principal
	mu   sync.Mutex
	file *os.File
	fd   int
	//TAmaño del archivo actual
	len atomic.Int64

	//indexMaster
	indexMaster *indexMaster

	//LOCALIZACION DE TODOS LOS INDICES
	indexLocation *PagedPool[Index]

	//INDICE SEARCH
	//unicamente para cluster de index
	indexSearch         map[[32]byte]IndexSearch
	indexSearchPool     chan uint32
	indexSearchDataPool *bufferArena

	//INDICES NORMALES
	//Indices libres para escritura
	indexPools map[uint32]chan uint32
	//Localizacion de los bufeer de los indices en un array, tamaño 4096
	indexBuffer *bufferArena

	//PAGINAS
	//Donde se localizan las paginas
	pageLocation *PagedPool[Page]

	//Mapa de nombres con direccion al array con los datos de cada archivo
	muPages sync.RWMutex
	pages map[[32]byte]int

	//Pool de buffers para los datos de las paginas
	dataPools map[int]*bufferArena

	//ESCRITURAS
	//pool de escritura para el wall
	dacV3WorkerWriter *dacV3WorkerWriter

	globalSizeSubIndex map[uint32]configIndex

	opts *DacV3Options
}

type SizeConfig struct {
	Size                uint32
	IndexSizeChan       uint32
	nBuffersAvaibleData uint32
}

func newDacV3(opts DacV3Options) *dacV3 {

	sfDacV3 := openFileDacV3(opts.dacRoute)

	sfDacV3.opts = &opts

	startHandleConfigIndexSize(sfDacV3)

	//Primeramente arrancar las operaciones de escritura en wall
	startHandleWallBuffer(sfDacV3)

	startHandleIndexMaster(sfDacV3)

	startHandleIndex(sfDacV3)

	startHandleData(sfDacV3)

	return sfDacV3
}

// AlignedBlock crea un buffer de tamaño 'size' alineado a 4096 bytes.
func MakeAlignedBlock(size int) []byte {

	buf := make([]byte, size+BufferAlignSize)

	ptr := uintptr(unsafe.Pointer(&buf[0]))

	offset := ptr & uintptr(BufferAlignSize-1)

	if offset != 0 {
		shift := BufferAlignSize - int(offset)

		return buf[shift : shift+size]
	}
	return buf[:size]
}
