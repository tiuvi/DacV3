package dacV3

func (sfDacV3 *DacV3) enqueueSync(j *jobWriter) error {

	pool := sfDacV3.dacV3WorkerWriter

	select {
	case pool.jobs <- j:

		select {
		case err := <-j.resp:
			return err
		case <-pool.ctx.Done():
			return pool.ctx.Err()
		}

	case <-pool.ctx.Done():
		return pool.ctx.Err()
	}
}

/*
ESCRITURA:
1º - Escribe primero un checksum en el wal e envia la respuesta como completado
2º - De manera asincrona en el siguiente ciclo de wal escribe los datos en su pagina original

RECUPERACION
  - se lee el indice wal y se recupera el checksum y se compara con el bloque de datos en la pagina original
    -si es positivo no se hace nada
    -Si es negativo los datos se borran

direct no es compatible con Batching
*/
func (sfDacV3 *DacV3) WriteDirectIo(hash [32]byte, relativeOffset int64, dataLen int64, idDataArena uint32, data []byte, offset int64) error {

	tasks := []jobWriterTask{
		{
			offset:         offset,
			idDataArena:    idDataArena,
			data:           data,
			hash:           hash,
			relativeOffset: relativeOffset,
			dataLen:        dataLen,
		},
	}

	j := &jobWriter{
		directIo: true,
		task:   tasks, // NUEVO: Asignamos el slice de tareas
		resp:   make(chan error, 1),
	}


	println("WriteDirect: " ,gettLastLine(string(j.task[0].data)) , " offset: ",j.task[0].relativeOffset)

	return sfDacV3.enqueueSync(j)
}

func (sfDacV3 *DacV3) WriteDirect(hash [32]byte, relativeOffset int64, dataLen int64, idDataArena uint32, data []byte, offset int64) error {

	tasks := []jobWriterTask{
		{
			offset:         offset,
			idDataArena:    idDataArena,
			data:           data,
			hash:           hash,
			relativeOffset: relativeOffset,
			dataLen:        dataLen,
		},
	}

	j := &jobWriter{
		direct: true,
		task:   tasks, // NUEVO: Asignamos el slice de tareas
		resp:   make(chan error, 1),
	}


	println("WriteDirect: " ,gettLastLine(string(j.task[0].data)) , " offset: ",j.task[0].relativeOffset)

	return sfDacV3.enqueueSync(j)
}

/*
ESCRITURA:
1º - Se escribe los datos en el wal y envia la respuesta como completado
2º - Despues manera asincrona en el siguiente ciclo de wal se escriben los datos en su pagina original

RECUPERACION
  - se lee el registro wal y se compara el checksum del indice con el checksum de los datos wall
    -Si es positivo continua
    -Si es negativo datos de wall corruptos no se llego a escribir los datos en la pagina original
  - se lee el bloque de datos en la pagina original y se hace checksum y se compara con los datos en el wal
    -Si es positivo no se hace nada
    -si es negativo se copia los datos del wal a la pagina original
*/

func (sfDacV3 *DacV3) WriteWall(hash [32]byte, relativeOffset int64, dataLen int64, idDataArena uint32, data []byte, offset int64) error {

	tasks := []jobWriterTask{
		{
			offset:         offset,
			idDataArena:    idDataArena,
			data:           data,
			hash:           hash,
			relativeOffset: relativeOffset,
			dataLen:        dataLen,
		},
	}

	j := &jobWriter{
		direct: false,
		task:   tasks, // NUEVO: Asignamos el slice de tareas
		resp:   make(chan error, 1),
	}

	if idDataArena == 0 {
		j.task[0].notDelIdDataArena = true
	}

	println("WriteWall: " ,gettLastLine(string(j.task[0].data)) , " offset: ",j.task[0].relativeOffset)

	return sfDacV3.enqueueSync(j)
}

func newWriterTask(idDataArena uint32, data []byte, offset int64) jobWriterTask {

	return jobWriterTask{
		idDataArena: idDataArena,
		data:        data,
		offset:      offset,
	}
}

func newWriterTaskOnce(data []byte, offset int64) jobWriterTask {

	return jobWriterTask{
		idDataArena:       0,
		notDelIdDataArena: true,
		data:              data,
		offset:            offset,
	}
}

// Escritura del wall por lotes
func (sfDacV3 *DacV3) WriteWallBath(tasks []jobWriterTask) error {

	j := &jobWriter{
		direct: false,
		task:   tasks, // NUEVO: Asignamos el slice de tareas
		resp:   make(chan error, 1),
	}

	return sfDacV3.enqueueSync(j)
}

/*
ESCRITURA
1º - Se escribe directamente los datos en el orgigen, esta funcion es de uso exclusivo interno con respuesta
*/
func (sfDacV3 *DacV3) WriteUnSafeSync(jTask *jobWriterTask) error {

	j := &jobWriter{
		directIo: true,
		direct:   false,
		task: []jobWriterTask{
			*jTask,
		},
		resp: make(chan error, 1),
	}

	//No se modifican hace falta para escribir directo
	//offset
	//notDelIdDataArena
	//idDataArena int64
	//data        []byte

	return sfDacV3.enqueueSync(j)
}

/*
ESCRITURA
1º - Se escribe directamente los datos en el orgigen, esta funcion es de uso exclusivo interno
*/
func (sfDacV3 *DacV3) WriteUnSafeAsync(jTask *jobWriterTask) {

	pool := sfDacV3.dacV3WorkerWriter

	jobWriterItem := &jobWriter{
		directIo: true,
		direct:   false,
		task: []jobWriterTask{
			*jTask,
		},
	}

	//EStas variables solo se usan al escribir en el buffer
	jobWriterItem.task[0].indexOffsetStart = 0
	jobWriterItem.task[0].indexOffsetEnd = 0

	//estas varialbles  se usan en writedisk , hay que ponerlas a cero para no modificar lo que escribe de buffer
	jobWriterItem.task[0].dataOffsetStart = 0
	jobWriterItem.task[0].dataOffsetEnd = 0

	//Deberia estar compleatdo
	//wg     sync.WaitGroup

	//ESto deberia estar completado ya
	//resp chan error

	//No se modifican hace falta para escribir directo
	//offset
	//notDelIdDataArena
	//idDataArena int64
	//data        []byte

	select {
	case <-pool.ctx.Done():
		println("ESCRITURA EN CANCELACION WriteUnSafeAsync")
		sfDacV3.processWriteUnSafe(jobWriterItem)
		return
	default:
	}

	select {
	case pool.jobs <- jobWriterItem:
		// Enviado con éxito
	case <-pool.ctx.Done():
		println("ESCRITURA EN CANCELACION WriteUnSafeAsync")
		sfDacV3.processWriteUnSafe(jobWriterItem)
	}
}

func (sfDacV3 *DacV3) WriteUnSafeAsyncPriority(jobWriterItem *jobWriter) {

	pool := sfDacV3.dacV3WorkerWriter

	select {
	case <-pool.ctx.Done():
		println("ESCRITURA EN CANCELACION WriteUnSafeAsyncPriority")
		sfDacV3.processWriteUnSafePriority(jobWriterItem)
		return
	default:
	}

	select {
	case pool.writeQueue <- jobWriterItem:
		// Enviado con éxito
	case <-pool.ctx.Done():
		println("ESCRITURA EN CANCELACION WriteUnSafeAsyncPriority")
		sfDacV3.processWriteUnSafePriority(jobWriterItem)
	}
}

func (sfDacV3 *DacV3) returnToThePriorityQueue(jobWriterItem *jobWriter) {

	pool := sfDacV3.dacV3WorkerWriter

	select {
	case <-pool.ctx.Done():

		if jobWriterItem.directIo {
			println("ESCRITURA EN CANCELACION returnToThePriorityQueue")
			sfDacV3.processWriteUnSafe(jobWriterItem)
		}
		println("ESCRITURA EN CANCELACION NO ENVIADO")
		return
	default:
	}

	select {
	case pool.jobs <- jobWriterItem:
		// Enviado con éxito
	case <-pool.ctx.Done():

		if jobWriterItem.directIo {
			println("ESCRITURA EN CANCELACION returnToThePriorityQueue")
			sfDacV3.processWriteUnSafe(jobWriterItem)
		}
		println("ESCRITURA EN CANCELACION NO ENVIADO")
	}
}
