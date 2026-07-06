package main

import (
	"context"
	"errors"

	"sync"
)

// --- ESTRUCTURAS ---
type jobWriterTask struct {
	offset int64

	notDelIdDataArena bool
	idDataArena       uint32
	data              []byte

	idIndexArena     uint32
	indexOffsetStart int64
	indexOffsetEnd   int64

	dataOffsetStart int64
	dataOffsetEnd   int64
}

type jobWriter struct {
	direct   bool
	directIo bool

	bufIdx int
	wg     sync.WaitGroup

	resp chan error
	task []jobWriterTask
}

type dacV3WorkerWriter struct {

	//Contexto global por si se cierra la aplicacion
	ctx    context.Context
	cancel context.CancelFunc

	queueSize int
	countJobs []int
	jobs      chan *jobWriter
	wg        sync.WaitGroup

	flushQueue chan *jobWriter
	flusherWg  sync.WaitGroup

	//Mutex para bloquear indexReserve y dataReserve
	mu sync.Mutex

	//Elige un buffer u otro
	chooseBuffer int

	//Variables de escritura en el buffer
	indexReserve     int64
	walLenIndex      int64
	wallBuffersIndex *bufferArena

	dataReserve     int64
	totalWallBuffer int
	wallLenBuffer   int64
	wallBuffers     [][]byte
}

func NewWorkerPool(numWorkers int, queueSize int, ssdNIopsMili uint32, wallBuffers [][]byte) *dacV3WorkerWriter {

	ctx, cancel := context.WithCancel(context.Background())

	totalWallBuffer := len(wallBuffers)

	pool := &dacV3WorkerWriter{
		ctx:        ctx,
		cancel:     cancel,
		jobs:       make(chan *jobWriter, queueSize),
		flushQueue: make(chan *jobWriter, queueSize),
		queueSize:  queueSize,

		countJobs: make([]int, totalWallBuffer),
		//Wallbuffers ya inicializados
		walLenIndex: int64(ssdNIopsMili * 4096),

		wallBuffersIndex: newBufferArena(ssdNIopsMili*uint32(totalWallBuffer), 4096),
		totalWallBuffer:  totalWallBuffer,

		wallLenBuffer: int64(len(wallBuffers[0])),
		wallBuffers:   wallBuffers,
	}

	pool.dataReserve = pool.walLenIndex

	// Arrancamos workers
	for i := 0; i < numWorkers; i++ {
		pool.wg.Add(1)
		go pool.worker()
	}

	// Arrancamos flusher
	pool.flusherWg.Add(1)
	go pool.flusher()

	return pool
}

func (pool *dacV3WorkerWriter) Stop() {

	close(pool.jobs)       // 1. Decimos a los workers que no hay más trabajos
	pool.wg.Wait()         // 2. Esperamos a que los workers terminen de encolar
	close(pool.flushQueue) // 3. Decimos al flusher que ya no llegarán más cosas a la cola
	pool.flusherWg.Wait()  // 4. Esperamos el flush final
	pool.cancel()
}

var ErrServerBusy = errors.New("servidor saturado")

func (pool *dacV3WorkerWriter) worker() {

	defer pool.wg.Done()

	for j := range pool.jobs {

		j.wg.Add(1)

		//Escritura directa sin soporte para batch
		if j.directIo {

			pool.mu.Lock()

			//Simulacion para que pase por flusher
			j.bufIdx = pool.chooseBuffer

			pool.countJobs[pool.chooseBuffer] += 1

			select {

			case pool.flushQueue <- j:

			default:

				pool.countJobs[pool.chooseBuffer] -= 1

				pool.mu.Unlock()

				j.wg.Done()

				pool.returnToThePriorityQueue(j)

				continue
			}

			pool.mu.Unlock()

			pool.processWriteUnSafe(j)

			j.wg.Done()

			continue
		}

		if j.direct {

			pool.mu.Lock()

			j.bufIdx = pool.chooseBuffer

			var totalIndexLen int64
			for range j.task {
				totalIndexLen += int64(BufferAlignSize)
			}

			if totalIndexLen+pool.indexReserve > pool.walLenIndex {

				pool.mu.Unlock()

				if j.resp != nil {
					j.resp <- ErrServerBusy
				}

				j.wg.Done()

				continue
			}

			pool.countJobs[pool.chooseBuffer] += 1

			if pool.countJobs[pool.chooseBuffer] >= pool.queueSize {

				pool.countJobs[pool.chooseBuffer] -= 1

				pool.mu.Unlock()

				if j.resp != nil {
					j.resp <- ErrServerBusy
				}

				j.wg.Done()

				continue
			}

			// Iteramos usando el índice 'i' para poder modificar el elemento original del slice
			for i := range j.task {

				// 1. Reservamos espacio para el INDEX de esta tarea individual
				j.task[i].indexOffsetStart = pool.indexReserve
				pool.indexReserve += int64(BufferAlignSize)
				j.task[i].indexOffsetEnd = pool.indexReserve

			}

			select {

			case pool.flushQueue <- j:

			default:

				pool.countJobs[pool.chooseBuffer] -= 1

				pool.indexReserve -= totalIndexLen

				pool.mu.Unlock()
				if j.resp != nil {
					j.resp <- ErrServerBusy
				}

				j.wg.Done()

				continue
			}

			pool.mu.Unlock()

			pool.processWriteBuffer(j)

			j.wg.Done()

			continue
		}

		pool.mu.Lock()

		j.bufIdx = pool.chooseBuffer

		lenWriteBuffer := int64(len(pool.wallBuffers[j.bufIdx]))

		// 1. Calculamos el tamaño TOTAL de los datos de todo el lote
		var totalDataLen int64
		var totalIndexLen int64
		for i := range j.task {
			totalDataLen += int64(len(j.task[i].data))
			totalIndexLen += int64(BufferAlignSize)
		}

		//2 validamos los indices
		if totalIndexLen+pool.indexReserve > pool.walLenIndex {

			pool.mu.Unlock()

			if j.resp != nil {
				j.resp <- ErrServerBusy
			}

			j.wg.Done()

			continue
		}

		// 3. Validamos si el lote completo (totalDataLen) cabe en el buffer
		if totalDataLen+pool.dataReserve > lenWriteBuffer {

			pool.mu.Unlock()

			if j.resp != nil {
				j.resp <- ErrServerBusy
			}

			j.wg.Done()

			continue
		}

		pool.countJobs[pool.chooseBuffer] += 1

		if pool.countJobs[pool.chooseBuffer] >= pool.queueSize {

			pool.countJobs[pool.chooseBuffer] -= 1

			pool.mu.Unlock()

			if j.resp != nil {
				j.resp <- ErrServerBusy
			}

			j.wg.Done()

			continue
		}

		// Iteramos usando el índice 'i' para poder modificar el elemento original del slice
		for i := range j.task {

			// 1. Reservamos espacio para el INDEX de esta tarea individual
			j.task[i].indexOffsetStart = pool.indexReserve
			pool.indexReserve += int64(BufferAlignSize)
			j.task[i].indexOffsetEnd = pool.indexReserve

			// 2. Calculamos el tamaño de la DATA de esta tarea individual
			lenTaskData := int64(len(j.task[i].data))

			// 3. Reservamos espacio para la DATA de esta tarea individual
			j.task[i].dataOffsetStart = pool.dataReserve
			pool.dataReserve += lenTaskData
			j.task[i].dataOffsetEnd = pool.dataReserve
		}

		select {

		case pool.flushQueue <- j:

		default:

			pool.countJobs[pool.chooseBuffer] -= 1

			pool.indexReserve -= totalIndexLen

			pool.dataReserve -= totalDataLen

			pool.mu.Unlock()

			if j.resp != nil {
				j.resp <- ErrServerBusy
			}

			j.wg.Done()

			continue
		}

		pool.mu.Unlock()

		pool.processWriteBuffer(j)

		j.wg.Done()
	}
}

func (pool *dacV3WorkerWriter) flusher() {

	defer pool.flusherWg.Done()

	// Inicializamos las estructuras dinámicamente según pool.totalWallBuffer
	batch := make([][]*jobWriter, pool.totalWallBuffer)

	for i := 0; i < pool.totalWallBuffer; i++ {
		batch[i] = make([]*jobWriter, 0, pool.queueSize)
	}

	for j := range pool.flushQueue {

		pool.mu.Lock()

		bufferAEnviarDisco := pool.chooseBuffer

		// --- ROTACIÓN CIRCULAR (Ring Buffer) ---
		pool.chooseBuffer = (pool.chooseBuffer + 1) % pool.totalWallBuffer

		//Los datos siempre empiezan a continuacion del indice
		pool.indexReserve = 0
		pool.dataReserve = pool.walLenIndex

		// 2. CLASIFICAR el trabajo 'j' que acaba de llegar
		if j.bufIdx == bufferAEnviarDisco {

			batch[bufferAEnviarDisco] = append(batch[bufferAEnviarDisco], j)

		} else {

			// Es de otro buffer, lo guardamos para luego
			println("error fatal posible corrupcion de buffer en flusher", j.bufIdx)
			if j.resp != nil {
				j.resp <- ErrServerBusy
			}
		}

		// 3. DRENAR LA COLA y clasificar
	LOOP:
		for len(batch[bufferAEnviarDisco]) < pool.queueSize {
			select {
			case nextJ := <-pool.flushQueue:

				if nextJ.bufIdx == bufferAEnviarDisco {

					batch[bufferAEnviarDisco] = append(batch[bufferAEnviarDisco], nextJ)
				} else {

					// Es de otro buffer, lo mandamos a pending
					println("error fatal posible corrupcion de buffer en flusher", j.bufIdx)
					nextJ.resp <- ErrServerBusy
				}

			default:
				break LOOP
			}
		}

		pool.countJobs[bufferAEnviarDisco] = 0

		pool.mu.Unlock()

		// 7. Esperamos a que todos los workers terminen de escribir en memoria
		if len(batch[bufferAEnviarDisco]) > 0 {

			for _, bJob := range batch[bufferAEnviarDisco] {
				bJob.wg.Wait()
			}

			// 8. Mandar a disco
			pool.processWriteDisk(batch[bufferAEnviarDisco], bufferAEnviarDisco)

			// Reseteamos el buffer
			for i := range pool.wallBuffers[bufferAEnviarDisco] {
				pool.wallBuffers[bufferAEnviarDisco][i] = 0
			}

			// Vaciamos el batch conservando la capacidad
			batch[bufferAEnviarDisco] = batch[bufferAEnviarDisco][:0]
		}

	}

	// FINAL FLUSH trabajos que no se pudieron encolar se pierden
	//No arriegar el actual wallbuffer 
	for i := 0; i < pool.totalWallBuffer; i++ {

		if len(batch[i]) > 0 {

			for _, jobs := range batch[i] {
				jobs.wg.Wait()
			}

		}
	}
}
