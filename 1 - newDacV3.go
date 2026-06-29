package main

import (
	"errors"
	"log"
	"os"
	"sync"
	"sync/atomic"
	"unsafe"

	"golang.org/x/sys/unix"
)

type dacV3 struct {
	//Gestion del archivo principal
	mu   sync.Mutex
	file *os.File
	fd   int
	//TAmaño del archivo actual
	len atomic.Int64

	//Gestion de indices
	//localizacion de los indices estructuras en un array
	indexLocation *PagedPool[Index]

	//Localizacion de los bufeer de los indices en un array
	indexBuffer *bufferArena

	//Indices libres para escritura
	indexPools map[int64]chan int64

	//variables para datos
	dataPools map[int]*bufferArena

	//Variables para escribir en el wall
	//tamaño del indice del wall
	walLenIndex int64
	//tamaño del los datos en el wall
	walLenData int64
	//tamaño total indicewall + datoswall
	walLenTotal int64
	//Todos los buferes wall que van rotando
	walBuffer [][]byte
	//pool de escritura para el wall
	dacV3WorkerWriter *dacV3WorkerWriter
}

func (sf *dacV3) ExpandSize(newSize int64) error {

	// Fast path sin bloquear (Lock-Free)
	if newSize <= sf.len.Load() {
		return nil
	}

	sf.mu.Lock()
	defer sf.mu.Unlock()

	// Doble comprobación tras adquirir el Lock
	if newSize <= sf.len.Load() {
		return nil
	}

	// mode = 0 (Sin KEEP_SIZE) para evitar actualizaciones de inodo en las escrituras posteriores
	if err := unix.Fallocate(sf.fd, 0, 0, newSize); err != nil {

		// Fallback si no está soportado (particiones antiguas)
		if !errors.Is(err, unix.EOPNOTSUPP) && !errors.Is(err, unix.ENOTSUP) {
			return err
		}

		if err := unix.Ftruncate(sf.fd, newSize); err != nil {
			return err
		}
	}

	// Actualización atómica para que los demás workers pasen por el Fast Path
	sf.len.Store(newSize)

	return nil
}

var globalDacV3 *dacV3

func newDacV3(ssdNIopsMili int64, ssdPageSize int64, totalWallBuffer int64, indexSize int64, supportedSizes []SizeConfig, nWorkers, queueSize int) *dacV3 {

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
		file:       dacV3Fd,
		fd:         fd,
		walBuffer:  make([][]byte, 2), // Reservamos espacio para 2 buffers []byte
		indexPools: make(map[int64]chan int64),
		dataPools:  make(map[int]*bufferArena),
	}

	sfDacV3.len.Store(size)

	//Inicio donde se guardan los indices
	sfDacV3.indexLocation = NewPagedPool[Index]()

	//Incicio de los buffers para los indices
	sfDacV3.indexBuffer = newBufferArena(indexSize, 4096)

	sizeIndex := ssdNIopsMili * 4096
	sfDacV3.walLenIndex = sizeIndex

	sizeData := ssdPageSize * ssdNIopsMili * 1000
	sfDacV3.walLenData = sizeData

	sfDacV3.walLenTotal = sizeIndex + sizeData

	walBuffer := newBufferArena(totalWallBuffer, sfDacV3.walLenTotal)
	for i := range totalWallBuffer {

		_, buf := walBuffer.addBufferArena()

		sfDacV3.walBuffer[i] = buf
	}

	//Inicio de los canales donde se van a guardar los indices
	for _, item := range supportedSizes {
		sfDacV3.indexPools[item.Size] = make(chan int64, item.IndexSizeChan)
	}

	for _, item := range supportedSizes {
		sfDacV3.dataPools[int(item.Size)] = newBufferArena(item.DataSize, item.Size)
	}

	reserveSizeWallBuffer := sfDacV3.walLenTotal * totalWallBuffer

	sfDacV3.ExpandSize(reserveSizeWallBuffer)

	sfDacV3.dacV3WorkerWriter = NewWorkerPool(nWorkers, queueSize, ssdNIopsMili, sfDacV3.walBuffer)

	globalDacV3 = sfDacV3

	return sfDacV3
}

const BufferAlignSize = 4096 // Tamaño de bloque típico en Linux (ext4/XFS)

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
