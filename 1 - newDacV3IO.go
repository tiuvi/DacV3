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
func (sfDacV3 *dacV3) WriteIndex(data []byte, offset int64) {

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
func (sfDacV3 *dacV3) WriteIndexMaster(data []byte, offset int64) error {

	buffer := MakeAlignedBlock(len(data))

	copy(buffer, data)

	return sfDacV3.WriteWall(0, buffer, offset)

}

var errArenaNotFound = errors.New("tamaño de arena no encontrado")

func (sfDacV3 *dacV3) WritePageDirect(data []byte, offset int64) error {

	arena, found := sfDacV3.dataPools[len(data)]
	if !found {
		return errArenaNotFound
	}

	id, buffer := arena.addBufferArena()

	copy(buffer, data)

	return sfDacV3.WriteDirect(id, buffer, offset)

}

// En esta parte obtener un buffer de escritura y eliminar idDataarena
func (sfDacV3 *dacV3) WritePageWall(data []byte, offset int64) error {

	arena, found := sfDacV3.dataPools[len(data)]
	if !found {
		return errArenaNotFound
	}

	id, buffer := arena.addBufferArena()

	copy(buffer, data)

	return sfDacV3.WriteWall(id, buffer, offset)

}
