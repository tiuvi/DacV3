package dacV3

import (
	"encoding/binary"
)

func (sfDacV3 *DacV3) processWriteUnSafePriority(j *jobWriter) {

	defer j.wg.Done()
	
		// Iteramos sobre cada tarea del lote
	for i := range j.task {

		if TestCrashEnergy == CrashWriteUnsafeCorrupt {

			if TestCrashTarget.Load() == j.task[i].relativeOffset+j.task[i].dataLen || TestCrashTarget.CompareAndSwap(-1, j.task[i].relativeOffset+j.task[i].dataLen) {

				clear(j.task[i].data[:30])

				// 2. Escribimos solo la mitad para simular el Torn Write
				sfDacV3.WriteAt(j.task[i].data, j.task[i].offset)

				// 3. ¡Le avisamos al hilo principal que ya hicimos el daño!
				TestCrashEnergiChan <- true

				// 4. Matamos el worker silenciosamente
				// runtime.Goexit()
				return

			}
		}

		println("processWriteUnSafePriority:  ", gettLastLine(string(j.task[i].data)), j.task[i].relativeOffset, j.task[i].offset)

		// 1. Escribimos la data de la tarea en su offset original en disco
		sfDacV3.WriteAt(j.task[i].data, j.task[i].offset)

		println("processWriteUnSafePriority finish:  ", gettLastLine(string(j.task[i].data)), j.task[i].relativeOffset)

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

func (sfDacV3 *DacV3) processWriteUnSafe(j *jobWriter) {

	// Iteramos sobre cada tarea del lote
	for i := range j.task {

		if TestCrashEnergy == CrashWriteUnsafeCorrupt {

			if TestCrashTarget.Load() == j.task[i].relativeOffset+j.task[i].dataLen || TestCrashTarget.CompareAndSwap(-1, j.task[i].relativeOffset+j.task[i].dataLen) {

				clear(j.task[i].data[:30])

				// 2. Escribimos solo la mitad para simular el Torn Write
				sfDacV3.WriteAt(j.task[i].data, j.task[i].offset)

				// 3. ¡Le avisamos al hilo principal que ya hicimos el daño!
				TestCrashEnergiChan <- true

				// 4. Matamos el worker silenciosamente
				// runtime.Goexit()
				return

			}
		}

		println("processWriteUnSafe:  ", gettLastLine(string(j.task[i].data)), j.task[i].relativeOffset, j.task[i].offset)

		// 1. Escribimos la data de la tarea en su offset original en disco
		sfDacV3.WriteAt(j.task[i].data, j.task[i].offset)

		println("processWriteUnSafe finish:  ", gettLastLine(string(j.task[i].data)), j.task[i].relativeOffset)

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

type bufWalControl []byte

const (
	// Offsets para la Secuencia (8 bytes)
	control_SequenceInit = 0
	control_SequenceEnd  = 8
)

// SetSequence escribe la secuencia uint64 en los primeros 8 bytes del bloque de control
func (c bufWalControl) SetSequence(seq uint64) {

	binary.LittleEndian.PutUint64(c[control_SequenceInit:control_SequenceEnd], seq)
}

// GetSequence lee la secuencia uint64 de los primeros 8 bytes del bloque de control
func (c bufWalControl) GetSequence() uint64 {

	return binary.LittleEndian.Uint64(c[control_SequenceInit:control_SequenceEnd])
}

// IsValid comprueba si el buffer es válido (secuencia >= 1)
func (c bufWalControl) IsEmpty() bool {

	return c.GetSequence() == 0
}

func (sfDacV3 *DacV3) processWriteDisk(batch []*jobWriter, chooseBuffer int) {

	pool := sfDacV3.dacV3WorkerWriter

	println("\n processWriteDisk operacion inicio:", chooseBuffer, pool.walSequence)
	secuenceSTart := pool.walSequence

	if len(batch) == 0 {
		return
	}

	// 1. Calculamos el tamaño máximo ocupado en el buffer
	// Debemos buscar el mayor dataOffsetEnd entre todas las tareas de todos los jobs
	var totalDataSize int64 = int64(BufferAlignSize)
	for _, j := range batch {

		println(j.bufIdx, j.directIo, j.direct)
		for i := range j.task {

			println("processWriteDisk: ", chooseBuffer, "sec", pool.walSequence, len(j.task[i].data), gettLastLine(string(j.task[i].data)), j.task[i].relativeOffset)
			println("comprobando: ", j.task[i].dataOffsetEnd, j.task[i].indexOffsetEnd)
			if j.task[i].dataOffsetEnd > totalDataSize {

				totalDataSize = j.task[i].dataOffsetEnd
			}

			if j.task[i].indexOffsetEnd > totalDataSize {

				totalDataSize = j.task[i].indexOffsetEnd
			}
		}
	}

	var activeTest bool
	if TestCrashEnergy == CrashWriteWallCorrupt || TestCrashEnergy == CrashWriteWallIndexCorrupt {

		for _, j := range batch {

			for i := range j.task {

				if TestCrashTarget.Load() == j.task[i].relativeOffset+j.task[i].dataLen || TestCrashTarget.CompareAndSwap(-1, j.task[i].relativeOffset+j.task[i].dataLen) {
					activeTest = true

					if TestCrashEnergy == CrashWriteWallCorrupt {

						//println("datos wal: ", string(pool.walBuffersTotal[chooseBuffer][j.task[i].dataOffsetStart:j.task[i].dataOffsetEnd]))
						view := pool.walBuffersTotal[chooseBuffer][j.task[i].dataOffsetStart:j.task[i].dataOffsetEnd]
						if len(view) > 32 {
							clear(view[32:])
						} else if len(view) > 0 {
							// Si mide 32 o menos, pero no está vacío, limpiamos lo que haya
							clear(view)
						}
						//println("datos wal: ", string(pool.walBuffersTotal[chooseBuffer][j.task[i].dataOffsetStart:j.task[i].dataOffsetEnd]))
					}

					if TestCrashEnergy == CrashWriteWallIndexCorrupt {

						//println("Creando un error en un indice de wal")
						view := pool.walBuffersTotal[chooseBuffer][j.task[i].indexOffsetStart:j.task[i].indexOffsetEnd]
						if len(view) >= 69 {
							clear(view[37:69])
						} else if len(view) > 0 {
							// Si mide 32 o menos, pero no está vacío, limpiamos lo que haya
							clear(view)
						}
					}
				}
			}
		}

	}

	// 2. Escribimos en el disco de manera síncrona el bloque del buffer usado

	// Escribimos la secuencia en el bloque de control del buffer (primeros 8 bytes)
	bufWalControl(pool.walBuffersTotal[chooseBuffer]).SetSequence(pool.walSequence)

	dataToWrite := pool.walBuffersTotal[chooseBuffer][:totalDataSize]

	offsetWrite := int64(chooseBuffer) * pool.walLenTotalBytes

	if activeTest {

		if TestCrashEnergy == CrashWriteWallCorrupt || TestCrashEnergy == CrashWriteWallIndexCorrupt {

			// 2. Escribimos solo la mitad para simular el Torn Write
			sfDacV3.WriteAtSync(dataToWrite, offsetWrite)

			for _, j := range batch {

				// Liberamos al cliente que hizo la petición síncrona (el canal está en el jobWriter padre)
				if j.resp != nil {
					j.resp <- nil
					close(j.resp)
				}
			}

			// 4. Matamos el worker silenciosamente
			// runtime.Goexit()
			return

		}

	}

	println("processWriteDisk operacion offsets:", offsetWrite, " size", len(dataToWrite))

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

		j.wg.Add(1)
		sfDacV3.WriteUnSafeAsyncPriority(j)
	}

	for _, j := range batch {
		j.wg.Wait()
	}

	println("processWriteDisk operacion fin:", chooseBuffer, secuenceSTart, "se suma uno:", pool.walSequence, "\n")
}
