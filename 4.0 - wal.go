package dacV3

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
	walLenIndexBytes int64

	dataReserve int64

	//Cantidad de bufferes que hay en rotacion
	numOfBuffersWal int
	//Tamaño total de un wal buffer
	walLenTotalBytes int64

	walSumBuffersSize int64
	walSequence       uint64

	//Son todos los buffers wal escribiendo en rotacion
	walBuffersTotal [][]byte
}

func (sfDacV3 *DacV3) NewWorkerPool(numWorkers int,
	queueSize int,
	walSequence uint64,
	walLenIndexBytes int64,
	numOfBuffersWal int,
	walLenTotalBytes int64,
	walBuffersTotal [][]byte) {

	ctx, cancel := context.WithCancel(context.Background())

	sfDacV3.dacV3WorkerWriter = &dacV3WorkerWriter{
		ctx:               ctx,
		cancel:            cancel,
		jobs:              make(chan *jobWriter, queueSize),
		flushQueue:        make(chan *jobWriter, queueSize),
		queueSize:         queueSize,
		numOfBuffersWal:   numOfBuffersWal,
		walSumBuffersSize: int64(numOfBuffersWal) * walLenTotalBytes,

		countJobs: make([]int, numOfBuffersWal),

		//walBuffersTotal ya inicializados
		walLenIndexBytes: walLenIndexBytes,

		walLenTotalBytes: walLenTotalBytes,
		walBuffersTotal:  walBuffersTotal,
	}

	pool := sfDacV3.dacV3WorkerWriter

	pool.indexReserve = int64(BufferAlignSize)
	pool.dataReserve = pool.walLenIndexBytes

	// Arrancamos workers
	for i := 0; i < numWorkers; i++ {
		pool.wg.Add(1)
		go sfDacV3.worker()
	}

	// Arrancamos flusher
	pool.flusherWg.Add(1)
	go sfDacV3.flusher()

	return
}

func (sfDacV3 *DacV3) Stop() {

	pool := sfDacV3.dacV3WorkerWriter

	close(pool.jobs)       // 1. Decimos a los workers que no hay más trabajos
	pool.wg.Wait()         // 2. Esperamos a que los workers terminen de encolar
	close(pool.flushQueue) // 3. Decimos al flusher que ya no llegarán más cosas a la cola
	pool.flusherWg.Wait()  // 4. Esperamos el flush final
	pool.cancel()
}

var ErrServerBusy = errors.New("server busy")

func (sfDacV3 *DacV3) worker() {

	pool := sfDacV3.dacV3WorkerWriter

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

				sfDacV3.returnToThePriorityQueue(j)

				continue
			}

			pool.mu.Unlock()

			sfDacV3.processWriteUnSafe(j)

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

			if totalIndexLen+pool.indexReserve > pool.walLenIndexBytes {

				pool.mu.Unlock()

				j.wg.Done()

				sfDacV3.returnToThePriorityQueue(j)

				continue
			}

			pool.countJobs[pool.chooseBuffer] += 1

			if pool.countJobs[pool.chooseBuffer] >= pool.queueSize {

				pool.countJobs[pool.chooseBuffer] -= 1

				pool.mu.Unlock()

				j.wg.Done()

				sfDacV3.returnToThePriorityQueue(j)

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

				j.wg.Done()

				sfDacV3.returnToThePriorityQueue(j)

				continue
			}

			pool.mu.Unlock()

			pool.processWriteBuffer(j)

			j.wg.Done()

			continue
		}

		pool.mu.Lock()

		j.bufIdx = pool.chooseBuffer

		lenWriteBuffer := int64(len(pool.walBuffersTotal[j.bufIdx]))

		// 1. Calculamos el tamaño TOTAL de los datos de todo el lote
		var totalDataLen int64
		var totalIndexLen int64
		for i := range j.task {
			totalDataLen += int64(len(j.task[i].data))
			totalIndexLen += int64(BufferAlignSize)
		}

		//2 validamos los indices
		if totalIndexLen+pool.indexReserve > pool.walLenIndexBytes {

			pool.mu.Unlock()

			j.wg.Done()

			sfDacV3.returnToThePriorityQueue(j)

			continue
		}

		// 3. Validamos si el lote completo (totalDataLen) cabe en el buffer
		if totalDataLen+pool.dataReserve > lenWriteBuffer {

			pool.mu.Unlock()

			j.wg.Done()

			sfDacV3.returnToThePriorityQueue(j)

			continue
		}

		pool.countJobs[pool.chooseBuffer] += 1

		if pool.countJobs[pool.chooseBuffer] >= pool.queueSize {

			pool.countJobs[pool.chooseBuffer] -= 1

			pool.mu.Unlock()

			j.wg.Done()

			sfDacV3.returnToThePriorityQueue(j)

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

			j.wg.Done()

			sfDacV3.returnToThePriorityQueue(j)

			continue
		}

		pool.mu.Unlock()

		pool.processWriteBuffer(j)

		j.wg.Done()
	}
}

func (sfDacV3 *DacV3) flusher() {

	pool := sfDacV3.dacV3WorkerWriter

	defer pool.flusherWg.Done()

	// Inicializamos las estructuras dinámicamente según pool.numOfBuffersWal
	batch := make([][]*jobWriter, pool.numOfBuffersWal)

	for i := 0; i < pool.numOfBuffersWal; i++ {
		batch[i] = make([]*jobWriter, 0, pool.queueSize)
	}

	for j := range pool.flushQueue {

		pool.mu.Lock()

		bufferAEnviarDisco := pool.chooseBuffer

		// --- ROTACIÓN CIRCULAR (Ring Buffer) ---
		pool.chooseBuffer = (pool.chooseBuffer + 1) % pool.numOfBuffersWal

		//Los datos siempre empiezan a continuacion del indice
		pool.indexReserve = int64(BufferAlignSize)
		pool.dataReserve = pool.walLenIndexBytes

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

		//println("¿Estamos agrupando writes?" ,len(batch[bufferAEnviarDisco]) )

		// 7. Esperamos a que todos los workers terminen de escribir en memoria
		if len(batch[bufferAEnviarDisco]) > 0 {

			for _, bJob := range batch[bufferAEnviarDisco] {
				bJob.wg.Wait()
			}

			// 8. Mandar a disco
			sfDacV3.processWriteDisk(batch[bufferAEnviarDisco], bufferAEnviarDisco)

			// Reseteamos el buffer
			for i := range pool.walBuffersTotal[bufferAEnviarDisco] {
				pool.walBuffersTotal[bufferAEnviarDisco][i] = 0
			}

			// Vaciamos el batch conservando la capacidad
			batch[bufferAEnviarDisco] = batch[bufferAEnviarDisco][:0]
		}

	}

	// FINAL FLUSH trabajos que no se pudieron encolar se pierden
	//No arriegar el actual wallbuffer
	for i := 0; i < pool.numOfBuffersWal; i++ {

		if len(batch[i]) > 0 {

			for _, jobs := range batch[i] {
				jobs.wg.Wait()
			}

		}
	}
}
