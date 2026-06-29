package main

import (
	"context"
	"errors"

	"sync"
)

// --- ESTRUCTURAS ---
type jobWriter struct {
	direct      bool
	offset      int64
	idDataArena int64
	data        []byte

	bufIdx int
	wg     sync.WaitGroup

	directIo bool

	idIndexArena int64

	indexOffsetStart int64
	indexOffsetEnd   int64

	dataOffsetStart int64
	dataOffsetEnd   int64

	resp chan error
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

func NewWorkerPool(numWorkers int, queueSize int, ssdNIopsMili int64, wallBuffers [][]byte) *dacV3WorkerWriter {

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
		walLenIndex: ssdNIopsMili * 4096,

		wallBuffersIndex: newBufferArena(ssdNIopsMili*int64(totalWallBuffer), 4096),
		totalWallBuffer:  totalWallBuffer,

		wallLenBuffer: int64(len(wallBuffers[0])),
		wallBuffers:   wallBuffers,
	}

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

		rejected := false


		if j.directIo {

			pool.mu.Lock()

			//Simulacion para que pase por flusher
			j.bufIdx = pool.chooseBuffer

			pool.countJobs[pool.chooseBuffer] += 1
			
			select {

			case pool.flushQueue <- j:

			default:

				rejected = true
				
				j.wg.Done()

				pool.countJobs[pool.chooseBuffer] -= 1

			}

			pool.mu.Unlock()

			if rejected {
				//Enviamos la tarea al pool de nuevo
				pool.WriteUnSafe(j)
				continue
			}

			pool.processWriteUnSafe(j)

			j.wg.Done()

			continue
		}

		

		pool.mu.Lock()

		j.bufIdx = pool.chooseBuffer

		lenWriteBuffer := int64(len(pool.wallBuffers[j.bufIdx]))

		lenJobData := int64(len(j.data))

		if lenJobData+pool.dataReserve > lenWriteBuffer {

			j.resp <- ErrServerBusy

			j.wg.Done()

			pool.mu.Unlock()

			continue
		}

		pool.countJobs[pool.chooseBuffer] += 1

		if pool.countJobs[pool.chooseBuffer] >= pool.queueSize {

			j.resp <- ErrServerBusy

			j.wg.Done()

			pool.mu.Unlock()

			continue
		}

		j.indexOffsetStart = pool.indexReserve
		pool.indexReserve += int64(BufferAlignSize)
		j.indexOffsetEnd = pool.indexReserve

		j.dataOffsetStart = pool.dataReserve
		pool.dataReserve += lenJobData
		j.dataOffsetEnd = pool.dataReserve

		select {

		case pool.flushQueue <- j:

		default:

			rejected = true

			j.wg.Done()

			pool.countJobs[pool.chooseBuffer] -= 1

			pool.indexReserve -= int64(BufferAlignSize)

			pool.dataReserve -= int64(len(j.data))

			j.resp <- ErrServerBusy

		}

		pool.mu.Unlock()

		if rejected {

			continue
		}

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
			j.resp <- ErrServerBusy
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
	for i := 0; i < pool.totalWallBuffer; i++ {

		if len(batch[i]) > 0 {

			for _, jobs := range batch[i] {
				jobs.wg.Wait()
			}

		}

	}
}
