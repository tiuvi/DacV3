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

	hash           [32]byte
	relativeOffset int64
	dataLen        int64
}

type jobWriter struct {
	direct   bool
	directIo bool

	bufIdx int
	wg     sync.WaitGroup

	resp chan error
	task []jobWriterTask

	//Para revertir el wall de paginas

}

type dacV3WorkerWriter struct {

	//Contexto global por si se cierra la aplicacion
	ctx    context.Context
	cancel context.CancelFunc

	ctxFlush    context.Context
	cancelFlush context.CancelFunc

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

	//nueva cola final
	ctxWriteQueue    context.Context
	cancelWriteQueue context.CancelFunc
	writeQueue       chan *jobWriter
	writeQueueWg     sync.WaitGroup
}

func (sfDacV3 *DacV3) NewWorkerPool(numWorkers int,
	queueSize int,
	walSequence uint64,
	walLenIndexBytes int64,
	numOfBuffersWal int,
	walLenTotalBytes int64,
	walBuffersTotal [][]byte) {

	ctx, cancel := context.WithCancel(context.Background())

	ctxFlush, cancelFlush := context.WithCancel(context.Background())

	ctxWriteQueue, cancelWriteQueue := context.WithCancel(context.Background())

	sfDacV3.dacV3WorkerWriter = &dacV3WorkerWriter{

		ctx:         ctx,
		cancel:      cancel,
		ctxFlush:    ctxFlush,
		cancelFlush: cancelFlush,

		ctxWriteQueue:    ctxWriteQueue,
		cancelWriteQueue: cancelWriteQueue,

		jobs:       make(chan *jobWriter, queueSize),
		flushQueue: make(chan *jobWriter, queueSize),

		writeQueue: make(chan *jobWriter, queueSize),

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

	for i := 0; i < numWorkers; i++ {
		pool.writeQueueWg.Add(1)
		go sfDacV3.workerWriter()
	}

}



func (sfDacV3 *DacV3) Stop() {

	pool := sfDacV3.dacV3WorkerWriter

	pool.cancel()

	pool.wg.Wait()

	pool.cancelFlush()

	pool.flusherWg.Wait() // 4. Esperamos el flush final

	pool.cancelWriteQueue()

	pool.writeQueueWg.Wait()

	//En este punto deberia de ser seguro cerrarlos
	close(pool.jobs)

	close(pool.flushQueue)

	close(pool.writeQueue)
}

var ErrServerBusy = errors.New("server busy")

func (sfDacV3 *DacV3) worker() {

	pool := sfDacV3.dacV3WorkerWriter

	defer pool.wg.Done()

	handleJob := func(jobWriterCurrent *jobWriter) {

		jobWriterCurrent.wg.Add(1)
		defer jobWriterCurrent.wg.Done()

		//Escritura directa sin soporte para batch
		if jobWriterCurrent.directIo {

			pool.mu.Lock()

			//Simulacion para que pase por flusher
			jobWriterCurrent.bufIdx = pool.chooseBuffer

			pool.countJobs[pool.chooseBuffer] += 1

			select {

			case pool.flushQueue <- jobWriterCurrent:

			default:

				pool.countJobs[pool.chooseBuffer] -= 1

				pool.mu.Unlock()

				sfDacV3.returnToThePriorityQueue(jobWriterCurrent)

				return
			}

			pool.mu.Unlock()

			sfDacV3.processWriteUnSafe(jobWriterCurrent)

			return
		}

		if jobWriterCurrent.direct {

			pool.mu.Lock()

			jobWriterCurrent.bufIdx = pool.chooseBuffer

			var totalIndexLen int64
			for range jobWriterCurrent.task {
				totalIndexLen += int64(BufferAlignSize)
			}

			if totalIndexLen+pool.indexReserve > pool.walLenIndexBytes {

				pool.mu.Unlock()

				sfDacV3.returnToThePriorityQueue(jobWriterCurrent)

				return
			}

			pool.countJobs[pool.chooseBuffer] += 1

			if pool.countJobs[pool.chooseBuffer] >= pool.queueSize {

				pool.countJobs[pool.chooseBuffer] -= 1

				pool.mu.Unlock()

				sfDacV3.returnToThePriorityQueue(jobWriterCurrent)

				return
			}

			// Iteramos usando el índice 'i' para poder modificar el elemento original del slice
			for i := range jobWriterCurrent.task {

				// 1. Reservamos espacio para el INDEX de esta tarea individual
				jobWriterCurrent.task[i].indexOffsetStart = pool.indexReserve
				pool.indexReserve += int64(BufferAlignSize)
				jobWriterCurrent.task[i].indexOffsetEnd = pool.indexReserve

			}

			select {

			case pool.flushQueue <- jobWriterCurrent:

			default:

				pool.countJobs[pool.chooseBuffer] -= 1

				pool.indexReserve -= totalIndexLen

				pool.mu.Unlock()

				sfDacV3.returnToThePriorityQueue(jobWriterCurrent)

				return
			}

			pool.mu.Unlock()

			pool.processWriteBuffer(jobWriterCurrent)

			return
		}

		pool.mu.Lock()

		jobWriterCurrent.bufIdx = pool.chooseBuffer

		lenWriteBuffer := int64(len(pool.walBuffersTotal[jobWriterCurrent.bufIdx]))

		// 1. Calculamos el tamaño TOTAL de los datos de todo el lote
		var totalDataLen int64
		var totalIndexLen int64
		for i := range jobWriterCurrent.task {
			totalDataLen += int64(len(jobWriterCurrent.task[i].data))
			totalIndexLen += int64(BufferAlignSize)
		}

		//2 validamos los indices
		if totalIndexLen+pool.indexReserve > pool.walLenIndexBytes {

			pool.mu.Unlock()

			sfDacV3.returnToThePriorityQueue(jobWriterCurrent)

			return
		}

		// 3. Validamos si el lote completo (totalDataLen) cabe en el buffer
		if totalDataLen+pool.dataReserve > lenWriteBuffer {

			pool.mu.Unlock()

			sfDacV3.returnToThePriorityQueue(jobWriterCurrent)

			return
		}

		pool.countJobs[pool.chooseBuffer] += 1

		if pool.countJobs[pool.chooseBuffer] >= pool.queueSize {

			pool.countJobs[pool.chooseBuffer] -= 1

			pool.mu.Unlock()

			sfDacV3.returnToThePriorityQueue(jobWriterCurrent)

			return
		}

		// Iteramos usando el índice 'i' para poder modificar el elemento original del slice
		for i := range jobWriterCurrent.task {

			// 1. Reservamos espacio para el INDEX de esta tarea individual
			jobWriterCurrent.task[i].indexOffsetStart = pool.indexReserve
			pool.indexReserve += int64(BufferAlignSize)
			jobWriterCurrent.task[i].indexOffsetEnd = pool.indexReserve

			// 2. Calculamos el tamaño de la DATA de esta tarea individual
			lenTaskData := int64(len(jobWriterCurrent.task[i].data))

			// 3. Reservamos espacio para la DATA de esta tarea individual
			jobWriterCurrent.task[i].dataOffsetStart = pool.dataReserve
			pool.dataReserve += lenTaskData
			jobWriterCurrent.task[i].dataOffsetEnd = pool.dataReserve
		}

		select {

		case pool.flushQueue <- jobWriterCurrent:

		default:

			pool.countJobs[pool.chooseBuffer] -= 1

			pool.indexReserve -= totalIndexLen

			pool.dataReserve -= totalDataLen

			pool.mu.Unlock()

			sfDacV3.returnToThePriorityQueue(jobWriterCurrent)

			return
		}

		pool.mu.Unlock()

		pool.processWriteBuffer(jobWriterCurrent)

	}

	for {
		select {
		case <-pool.ctx.Done():

			for len(pool.jobs) > 0 {

				select {
				case job, ok := <-pool.jobs:
					if ok {
						handleJob(job)
					}
				default:
					break
				}
			}
			return

		case jobWriterIncoming, ok := <-pool.jobs:
			if !ok {
				return
			}
			handleJob(jobWriterIncoming)

		}
	}

}


func (sfDacV3 *DacV3) workerWriter() {

		pool := sfDacV3.dacV3WorkerWriter

		defer pool.writeQueueWg.Done()

		handleJob := func(job *jobWriter){
			sfDacV3.processWriteUnSafePriority(job)
		}

		for {
		select {
		case <-pool.ctxWriteQueue.Done():

			for len(pool.writeQueue) > 0 {

				select {
				case job, ok := <-pool.writeQueue:
					if ok {
						handleJob(job)
					}
				default:
					break
				}
			}
			return

		case jobWriterIncoming, ok := <-pool.writeQueue:
			if !ok {
				return
			}
			handleJob(jobWriterIncoming)

		}
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

	handleFunc := func(jobWriterCurrent *jobWriter) {

		pool.mu.Lock()

		bufferAEnviarDisco := pool.chooseBuffer

		// --- ROTACIÓN CIRCULAR (Ring Buffer) ---
		pool.chooseBuffer = (pool.chooseBuffer + 1) % pool.numOfBuffersWal
		//aumentamos la secuencia
		pool.walSequence = pool.walSequence + 1

		//Los datos siempre empiezan a continuacion del indice
		pool.indexReserve = int64(BufferAlignSize)
		pool.dataReserve = pool.walLenIndexBytes

		// 2. CLASIFICAR el trabajo 'jobWriterIncoming' que acaba de llegar
		if jobWriterCurrent.bufIdx == bufferAEnviarDisco {

			batch[bufferAEnviarDisco] = append(batch[bufferAEnviarDisco], jobWriterCurrent)

		} else {

			// Es de otro buffer, lo guardamos para luego
			println("error fatal posible corrupcion de buffer en flusher", jobWriterCurrent.bufIdx)
			if jobWriterCurrent.resp != nil {
				jobWriterCurrent.resp <- ErrServerBusy
			}
		}

		// 3. DRENAR LA COLA y clasificar
	LOOP:
		for len(batch[bufferAEnviarDisco]) < pool.queueSize {

			select {
			case nextJ, ok := <-pool.flushQueue:

				if !ok {
					break LOOP
				}

				if nextJ.bufIdx == bufferAEnviarDisco {

					batch[bufferAEnviarDisco] = append(batch[bufferAEnviarDisco], nextJ)

					println("FLUSHER nextJ handlefunc", bufferAEnviarDisco, len(nextJ.task[0].data), gettLastLine(string(nextJ.task[0].data)), nextJ.task[0].relativeOffset)

				} else {

					// Es de otro buffer, lo mandamos a pending
					println("error fatal posible corrupcion de buffer en flusher", nextJ.bufIdx)
					if nextJ.resp != nil { // 👈 COMPROBACIÓN NULA AGREGADA
						nextJ.resp <- ErrServerBusy
					}
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
			println("EJECUTANDO processWriteDisk: ", bufferAEnviarDisco)
			sfDacV3.processWriteDisk(batch[bufferAEnviarDisco], bufferAEnviarDisco)

			// Reseteamos el buffer
			for i := range pool.walBuffersTotal[bufferAEnviarDisco] {
				pool.walBuffersTotal[bufferAEnviarDisco][i] = 0
			}

			// Vaciamos el batch conservando la capacidad
			batch[bufferAEnviarDisco] = batch[bufferAEnviarDisco][:0]
		}

	}

FLUSHER_LOOP:
	for {

		var jobWriterIncoming *jobWriter
		var ok bool

		select {
		case <-pool.ctxFlush.Done():
			// 🚨 DRENADO FINAL (GRACEFUL SHUTDOWN)
			// La DB se está apagando. Usamos handleFunc para vaciar
			// todo lo que haya quedado haciendo fila en la cola.
			for len(pool.flushQueue) > 0 {
				if item, ok := <-pool.flushQueue; ok {

					handleFunc(item)
				}
			}
			// Una vez vaciada la cola, salimos y la goroutine muere.
			break FLUSHER_LOOP

		case jobWriterIncoming, ok = <-pool.flushQueue:
			// 2. Revisamos si el canal fue cerrado explícitamente.
			if !ok {
				break FLUSHER_LOOP
			}
			println("FLUSHER handlefunc", len(jobWriterIncoming.task[0].data), gettLastLine(string(jobWriterIncoming.task[0].data)), jobWriterIncoming.task[0].relativeOffset)

			handleFunc(jobWriterIncoming)
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
