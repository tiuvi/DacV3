package dacV3

import "encoding/binary"

func (sfDacV3 *dacV3) processWriteUnSafe(j *jobWriter) {

	// Iteramos sobre cada tarea del lote
	for i := range j.task {

		// 1. Escribimos la data de la tarea en su offset original en disco
		sfDacV3.WriteAt(j.task[i].data, j.task[i].offset)

		// 2. Si esta tarea no requiere borrar la arena, pasamos a la SIETE tarea
		// ¡IMPORTANTE!: Usamos 'continue' en lugar de 'return' para no abortar el resto del batch
		if j.task[i].notDelIdDataArena {
			continue
		}

		// 3. Eliminamos la arena de memoria correspondiente a esta tarea
		mapArena := sfDacV3.writeDataPools[len(j.task[i].data)]

		mapArena.delBufferArena(j.task[i].idDataArena)
	}

}

func (sfDacV3 *dacV3) processWriteDisk(batch []*jobWriter, chooseBuffer int) {

	pool := sfDacV3.dacV3WorkerWriter

	if len(batch) == 0 {
		return
	}

	// 1. Calculamos el tamaño máximo ocupado en el buffer
	// Debemos buscar el mayor dataOffsetEnd entre todas las tareas de todos los jobs
	var totalDataSize int64
	for _, j := range batch {

		for i := range j.task {
			if j.task[i].dataOffsetEnd > totalDataSize {
				totalDataSize = j.task[i].dataOffsetEnd
			}
			if j.task[i].indexOffsetEnd > totalDataSize {
				totalDataSize = j.task[i].indexOffsetEnd
			}
		}
	}

	// 2. Escribimos en el disco de manera síncrona el bloque del buffer usado

	// Escribimos la secuencia en el bloque de control del buffer (primeros 8 bytes)
	binary.BigEndian.PutUint64(pool.walBuffersTotal[chooseBuffer][0:8], pool.walSequence)
	pool.walSequence++

	dataToWrite := pool.walBuffersTotal[chooseBuffer][:totalDataSize]

	offsetWrite := int64(chooseBuffer) * pool.walLenTotalBytes

	sfDacV3.WriteAtSync(dataToWrite, offsetWrite)

	// 3. Liberamos la espera de los clientes y encolamos la escritura asíncrona a sus páginas
	for _, j := range batch {

		// Liberamos al cliente que hizo la petición síncrona (el canal está en el jobWriter padre)
		if j.resp != nil {
			j.resp <- nil
			close(j.resp)
		}

		if j.directIo {
			continue
		}

		if j.bufIdx != chooseBuffer {
			println("ERROR FATAL Condicion de carrera, escribiendo: ", chooseBuffer, " Buffer equivocado: ", j.bufIdx)
		}

		// NUEVO: Limpiamos la arena de índices para CADA tarea individual de este job
		for i := range j.task {

			sfDacV3.WriteUnSafeAsync(&j.task[i])
		}

	}

}
