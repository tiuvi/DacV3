package main

import (
	"golang.org/x/sys/unix"
)

func (pool *dacV3WorkerWriter) processWriteUnSafe(j *jobWriter) {

	_, err := globalDacV3.file.WriteAt(j.data, j.offset)
	if err != nil {

		println("ERROR FATAL - directIo - processWriteBuffer: ", err.Error())

		pool.Stop()

		return
	}

	mapArena, found := globalDacV3.dataPools[len(j.data)]
	if !found {
		println("ERROR FATAL - directIo - processWriteBuffer - mapArena no encontrado")
		return
	}

	mapArena.delBufferArena(j.idDataArena)

}

func (pool *dacV3WorkerWriter) processWriteDisk(batch []*jobWriter, chooseBuffer int) {

	if len(batch) == 0 {
		return
	}

	var totalDataSize int64
	for _, j := range batch {

		if j.dataOffsetEnd > totalDataSize {
			totalDataSize = j.dataOffsetEnd
		}
	}

	dataToWrite := pool.wallBuffers[chooseBuffer][:totalDataSize]
	offsetWrite := int64(chooseBuffer) * pool.wallLenBuffer

	_, err := globalDacV3.file.WriteAt(dataToWrite, offsetWrite)
	if err != nil {

		println("ERROR FATAL - processWriteDisk: ", err.Error())

		pool.Stop()

		return
	}

	err = unix.Fdatasync(int(globalDacV3.file.Fd()))
	if err != nil {

		println("ERROR FATAL - processWriteDisk (Fdatasync): ", err.Error())

		pool.Stop()
		
		return
	}

	// Liberamos la espera de AddWorkerJobSync
	for _, j := range batch {

		if j.directIo {
			continue
		}

		if j.bufIdx != chooseBuffer {
			println("ERROR FATAL Condicion de carrera, escribiendo: ", chooseBuffer, " Buffer equivocado: ", j.bufIdx)
		}

		if j.resp != nil {

			j.resp <- nil

			close(j.resp)
		}

		//limpiar indexbuffer aqui
		pool.wallBuffersIndex.delBufferArena(j.idIndexArena)

		pool.WriteUnSafe(j)
	}

}
