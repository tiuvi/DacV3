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

type DacV3 struct {
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
	indexSearch map[[32]byte]IndexSearch
	//indexSearch ConcurrentMap[[32]byte , IndexSearch]

	indexSearchPool     chan IndexPoolItem
	indexSearchDataPool *bufferArena
	indexAvailableSlotsSearch *atomic.Int64

	//INDICES NORMALES
	//Indices libres para escritura
	indexPools map[uint32]chan IndexPoolItem
	//Localizacion de los bufeer de los indices en un array, tamaño 4096
	indexBuffer *bufferArena
	indexAvailableSlots map[uint32]*atomic.Int64

	//PAGINAS
	//Donde se localizan las paginas
	pageLocation *PagedPool[Page]

	//Mapa de nombres con direccion al array con los datos de cada archivo
	pages *ConcurrentMap[uint32]

	//Pool de buffers para los datos de las paginas
	dataPools map[int]*GlobalBufferPool

	writeDataPools map[int]*bufferArena

	//ESCRITURAS
	//pool de escritura para el wall
	dacV3WorkerWriter *dacV3WorkerWriter

	globalSizeSubIndex map[uint32]configIndex

	globalSizeIndex []uint32

	opts *DacV3Options

	//Un canal donde se solicita mas indices
	needIndexChan chan newIndexRequest
}

type newIndexRequest struct {
    sizePagination int64
    isSearch       bool
}

type IndexPoolItem struct {
    IDIndex        uint32
    AvailableSlots uint8 // Número de slots vacíos disponibles en este índice
}

type SizeConfig struct {
	Size                uint32
	IndexSizeChan       uint32
	NBuffersAvaibleData uint32
}

// DefaultDacV3Options devuelve la configuración por defecto para DacV3.
func NewDacV3Options(diskPath string, truncate bool, multiplierChan uint32) DacV3Options {

	return DacV3Options{
		DacRoute:        diskPath,
		SizeIndexMaster: 4096,              // multiplos de 4096
		MaxReserveSize:  1024 * 1024 * 100, // 100 MB
		SsdNIopsMili:    100,
		Truncate:        truncate,

		//Opciones para indices normales
		NBuffersAvailableIndex: 128,

		//Opciones para indexSearch dinamicos
		NBuffersAvailableIndexSearch:     8,
		NChanAvaibleIndexSearch:          1 * multiplierChan,
		NBuffersAvailableIndexSearchData: MaxSubIndexPerIndex * multiplierChan,

		queueChanMultiplier:100,
		minPercentajeTotalSlotsCreate: 10,

		//Indices soportados
		SupportedSizes: []SizeConfig{
			{
				//tamaño del indice
				Size:                4096,
				//Cuantos indices de este tamaños estan disponibles
				IndexSizeChan:       16 * multiplierChan,
				//Cuantos buffer de datos de este tamaño estan disponibles
				NBuffersAvaibleData: MaxSubIndexPerIndex * 16,
			},
			{
				Size:                16384,
				IndexSizeChan:       4 * multiplierChan,
				NBuffersAvaibleData: MaxSubIndexPerIndex * 4,
			},
			{
				Size:                32768,
				IndexSizeChan:       2 * multiplierChan,
				NBuffersAvaibleData: MaxSubIndexPerIndex * 2,
			},
			{
				Size:                65536,
				IndexSizeChan:       1 * multiplierChan,
				NBuffersAvaibleData: MaxSubIndexPerIndex,
			},
		},
		//Numero de workers escribiendo de forma concurrente
		NWorkers:  32,
		//Tamaño de la cola del worker
		QueueSize: 16384,
	}
}

func InitDacV3(opts DacV3Options) *DacV3 {

	sfDacV3 := openFileDacV3(opts.DacRoute, opts.Truncate)

	//Worker para crear indices sin bloqueo
	sfDacV3.needIndexChan = make(chan newIndexRequest, 512)

	go sfDacV3.newIndexsManagerWorker()

	sfDacV3.opts = &opts

	startHandleConfigIndexSize(sfDacV3)

	startHandleIndexMaster(sfDacV3)

	//Primeramente arrancar las operaciones de escritura en wall
	startHandleWallBuffer(sfDacV3)

	initIndexMasterBuffers(sfDacV3)

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
