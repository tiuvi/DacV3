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

	//Gestion de indices
	//localizacion de los indices estructuras en un array
	indexLocation *PagedPool[Index]

	//unicamente para cluster de index
	indexSearch         map[[32]byte]IndexSearch
	indexSearchPool     chan uint32
	indexSearchDataPool *bufferArena
	indexBufferDouble   []byte
	//Localizacion de los bufeer de los indices en un array
	indexBuffer *bufferArena

	//Indices libres para escritura
	indexPools map[uint32]chan uint32

	//Se usa int para no tener que hacer conversiones al optener la pagina
	dataPools map[int]*bufferArena

	//pool de escritura para el wall
	dacV3WorkerWriter *dacV3WorkerWriter

	opts *DacV3Options
}

type SizeConfig struct {
	Size                uint32
	IndexSizeChan       uint32
	nBuffersAvaibleData uint32
}

func newDacV3(opts DacV3Options) *dacV3 {

	sfDacV3 := openFileDacV3()

	sfDacV3.opts = &opts

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
