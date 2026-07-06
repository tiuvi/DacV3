package dacV3

import (
	"log"
	"os"
	"sync"
	"sync/atomic"
	"unsafe"

	"golang.org/x/sys/unix"
)

type configIndex struct {
	blocks          int64
	sizeBlock       int64
	pages           int64
	pageSize        int64
	pageStartOffset int64
}

type indexMaster struct {

	//Este bloque es el tamaño minimo por bloque
	blockMinSize int64

	sizeSubIndex map[uint32]configIndex
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
	indexSearch map[[32]byte]int

	//Localizacion de los bufeer de los indices en un array
	indexBuffer *bufferArena

	//Indices libres para escritura
	indexPools map[uint32]chan uint32

	//variables para datos
	dataPools map[int]*bufferArena

	//Variables para escribir en el wall

	//tamaño total indicewall + datoswall
	walLenTotal int64
	//Todos los buferes wall que van rotando
	walBuffer [][]byte
	//pool de escritura para el wall
	dacV3WorkerWriter *dacV3WorkerWriter
}

type SizeConfig struct {
	Size                uint32
	IndexSizeChan       uint32
	nBuffersAvaibleData uint32
}

func openFileDacV3() *dacV3 {

	fd, err := unix.Open("/media/franky/tiuviweb/test/4 - dacV3/files", unix.O_RDWR|unix.O_CREAT|unix.O_DIRECT, 0666)
	if err != nil {
		// Manejar el error de forma oportuna
		log.Fatalf("Error al abrir el archivo: %v", err)
	}

	// Convertimos fd (int) a uintptr de manera explícita
	dacV3Fd := os.NewFile(uintptr(fd), "dacv3")

	size, err := dacV3Fd.Seek(0, 2)
	if err != nil {
		// Manejar el error de forma oportuna
		log.Fatalf("Error al abrir el archivo: %v", err)
	}

	sfDacV3 := &dacV3{
		file: dacV3Fd,
		fd:   fd,
	}

	sfDacV3.len.Store(size)

	return sfDacV3
}

const BufferAlignSize = 4096 // Tamaño de bloque típico en Linux (ext4/XFS)
// 4 bloques fijos de 4096 perdidos para índices
const IndexOverheadBlocks int64 = 4

// maximo de subindices por indice 4096
const maxSubIndexPerIndex = 98

var globalSizeSubIndex map[uint32]configIndex

// init se ejecuta automáticamente al iniciar el programa, antes que main()
func init() {
	globalSizeSubIndex = make(map[uint32]configIndex)

	// Iteramos desde 1 hasta 16 para abarcar desde 4096 hasta 65536
	for multiplier := int64(1); multiplier <= 32; multiplier++ {

		pageSize := multiplier * BufferAlignSize
		blocks := multiplier * maxSubIndexPerIndex

		// Aseguramos que haya suficientes bloques para descontar la cabecera
		var pages int64 = 0
		if blocks > IndexOverheadBlocks {
			pages = (blocks - IndexOverheadBlocks) / multiplier
		}

		// CORRECCIÓN: Tamaño total del bloque - (Cantidad de páginas * Tamaño de cada página)
		// De esta forma, garantizamos que los datos terminan exactamente en el último byte.
		startOffset := (blocks * BufferAlignSize) - (pages * pageSize)

		globalSizeSubIndex[uint32(pageSize)] = configIndex{
			blocks:          blocks,
			sizeBlock:       blocks * BufferAlignSize,
			pages:           pages,
			pageSize:        pageSize,
			pageStartOffset: startOffset,
		}
	}
}

/*
Cambiar esta funcion
1º reservar datos para el wal
2º reservar datos para el indice
3º reservar datos para los datos
*/
func (sfDacV3 *dacV3) startHandleIndexMaster(maxReserveSize int64, supportedSizes []SizeConfig) {

	indexMaster := indexMaster{}

	sfDacV3.indexMaster = &indexMaster

	sfDacV3.indexMaster.sizeSubIndex = make(map[uint32]configIndex)

	for _, item := range supportedSizes {

		data, found := globalSizeSubIndex[item.Size]
		if !found {
			log.Fatal("Tamaño de indice no compatible")
		}

		sfDacV3.indexMaster.sizeSubIndex[item.Size] = data

		if sfDacV3.indexMaster.blockMinSize > int64(data.pageSize) {
			sfDacV3.indexMaster.blockMinSize = data.pageSize
		}
	}

}

func (sfDacV3 *dacV3) startHandleIndex(nBuffersAvaibleIndex uint32, supportedSizes []SizeConfig) {

	//Inicio donde se guardan los indices
	sfDacV3.indexLocation = NewPagedPool[Index]()

	sfDacV3.indexSearch = make(map[[32]byte]int)

	//Incicio de los buffers para los indices
	sfDacV3.indexBuffer = newBufferArena(nBuffersAvaibleIndex, BufferAlignSize)

	sfDacV3.indexPools = make(map[uint32]chan uint32)
	//Inicio de los canales donde se van a guardar los indices
	for _, item := range supportedSizes {
		sfDacV3.indexPools[item.Size] = make(chan uint32, item.IndexSizeChan)
	}

}

func (sfDacV3 *dacV3) startHandleData(supportedSizes []SizeConfig) {

	sfDacV3.dataPools = make(map[int]*bufferArena)
	for _, item := range supportedSizes {
		sfDacV3.dataPools[int(item.Size)] = newBufferArena(item.nBuffersAvaibleData, int64(item.Size))
	}

}

func (sfDacV3 *dacV3) startHandleWallBuffer(ssdNIopsMili uint32, ssdPageSize uint32, totalWallBuffer uint32, nWorkers, queueSize int) {

	//Operaciones por milisegundo por tamaño de un indice en el wall
	sizeIndex := ssdNIopsMili * 4096

	sizeData := ssdPageSize * ssdNIopsMili * 1000

	sfDacV3.walLenTotal = int64(sizeIndex + sizeData)

	sfDacV3.walBuffer = make([][]byte, totalWallBuffer)

	walBuffer := newBufferArena(totalWallBuffer, sfDacV3.walLenTotal)

	for i := range totalWallBuffer {

		_, buf := walBuffer.addBufferArena()

		sfDacV3.walBuffer[i] = buf
	}

	sfDacV3.dacV3WorkerWriter = NewWorkerPool(nWorkers, queueSize, ssdNIopsMili, sfDacV3.walBuffer)

}

var globalDacV3 *dacV3

func newDacV3(opts DacV3Options) *dacV3 {

	sfDacV3 := openFileDacV3()

	sfDacV3.startHandleIndexMaster(opts.MaxReserveSize, opts.SupportedSizes)

	sfDacV3.startHandleIndex(opts.NBuffersAvailableIndex, opts.SupportedSizes)

	sfDacV3.startHandleData(opts.SupportedSizes)

	sfDacV3.startHandleWallBuffer(opts.SsdNIopsMili, opts.SsdPageSize, opts.TotalWallBuffer, opts.NWorkers, opts.QueueSize)

	// Tarea de expandir el buffer para almacenar indices
	reserveSizeWallBuffer := sfDacV3.walLenTotal * int64(opts.TotalWallBuffer)

	sfDacV3.ExpandSize(int64(reserveSizeWallBuffer))

	globalDacV3 = sfDacV3

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
