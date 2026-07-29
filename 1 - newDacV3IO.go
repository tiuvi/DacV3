package dacV3

import (
	"errors"
	"log"
	"time"
)

/*
-Esta funcion escribe los datos directos en el origen sin validacion de ningun tipo.
-
*/
func (sfDacV3 *DacV3) WriteIndex(data []byte, offset int64) {

	job := &jobWriterTask{
		offset:            offset,
		notDelIdDataArena: true,
		data:              data,
	}

	maxRetries := 10
	var err error
	for i := 0; i < maxRetries; i++ {

		err = sfDacV3.WriteUnSafeSync(job)
		if err == nil {
			break
		}

		time.Sleep(1 * time.Millisecond)
	}

	if err != nil {
		log.Fatal("ERROR WriteIndex", err.Error())
	}

	return
}

/*
- Escribe los datos usando el wall pero con un buffer propio
*/
var hashZero [32]byte

func (sfDacV3 *DacV3) WriteIndexMaster(data []byte, offset int64) error {

	buffer := MakeAlignedBlock(len(data))

	copy(buffer, data)

	return sfDacV3.WriteWall(hashZero, 0, 0, 0, buffer, offset)

}

var errArenaNotFound = errors.New("tamaño de arena no encontrado")

// WritePageDirect escribe datos de forma directa utilizando los buffers de la arena.
// onCopy (opcional) se invoca tan pronto como la copia en memoria finaliza,
// permitiendo al invocador liberar mutexes u otros bloqueos antes del I/O.
func (sfDacV3 *DacV3) WritePageDirect(hash [32]byte, relativeOffset int64,dataLen int64, data []byte, offset int64, onCopy func()) error {

	arena, found := sfDacV3.writeDataPools[len(data)]
	if !found {
		// Importante: liberar el bloqueo superior incluso en caso de error temprano
		// para evitar posibles deadlocks.
		if onCopy != nil {
			onCopy()
		}
		return errArenaNotFound
	}

	// 1. Solicitamos un bloque de memoria seguro desde el pool
	id, buffer := arena.addBufferArena()

	// 2. Realizamos la copia de los bytes de forma rápida en memoria
	copy(buffer, data)

	// 3. Notificamos al invocador que los datos ya están en nuestro dominio
	// para que pueda liberar sus Mutex o reciclar 'data'.
	if onCopy != nil {
		onCopy()
	}

	// 4. Procedemos con la escritura directa a disco (potencial bloqueo por I/O)
	return sfDacV3.WriteDirect(hash, relativeOffset,dataLen, id, buffer, offset)
}

func (sfDacV3 *DacV3) WritePageWall(hash [32]byte, relativeOffset int64, dataLen int64, data []byte, offset int64, onCopy func()) error {

	arena, found := sfDacV3.writeDataPools[len(data)]
	if !found {
		// Si es obligatorio liberar el mutex, lo hacemos incluso si hay error
		if onCopy != nil {
			onCopy()
		}
		return errArenaNotFound
	}

	// 1. Obtenemos un bloque libre de la arena (rápido)
	id, buffer := arena.addBufferArena()

	// 2. Copiamos la memoria RAM a RAM (muy rápido)
	copy(buffer, data)

	// 3. Avisamos al invocador: "Ya no necesito 'data', puedes soltar el Mutex"
	if onCopy != nil {
		onCopy()
	}

	// 4. Escribimos al disco/WAL (Lento - I/O Blocking)
	// Aquí ya no retenemos los locks de la capa superior.
	return sfDacV3.WriteWall(hash, relativeOffset, dataLen, id, buffer, offset)
}
