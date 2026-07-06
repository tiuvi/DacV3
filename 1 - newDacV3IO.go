package main

import "errors"

/*
-Esta funcion para nuevo archivo sin datos
*/
func (sfDacV3 *dacV3) WriteIndex(data []byte, offset int64) error {

	job := &jobWriterTask{
		offset:            offset,
		notDelIdDataArena: true,

		data: data,
	}

	return sfDacV3.dacV3WorkerWriter.WriteUnSafeSync(job)
}

var errArenaNotFound = errors.New("tamaño de arena no encontrado")

func (sfDacV3 *dacV3) WritePageDirect(data []byte, offset int64) error {

	arena, found := sfDacV3.dataPools[len(data)]
	if !found {
		return errArenaNotFound
	}

	id, buffer := arena.addBufferArena()

	copy(buffer, data)

	return sfDacV3.dacV3WorkerWriter.WriteDirect(id, buffer, offset)

}

// En esta parte obtener un buffer de escritura y eliminar idDataarena
func (sfDacV3 *dacV3) WritePageWall(idDataArena int64, data []byte, offset int64) error {

	arena, found := sfDacV3.dataPools[len(data)]
	if !found {
		return errArenaNotFound
	}

	id, buffer := arena.addBufferArena()

	copy(buffer, data)

	return sfDacV3.dacV3WorkerWriter.WriteWall(id, buffer, offset)

}
