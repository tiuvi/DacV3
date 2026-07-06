package main

import (
	"encoding/binary"
	"errors"
	"hash/crc32"
)

var castagnoliTable = crc32.MakeTable(crc32.Castagnoli)

const (
	field_WalCheckSumInit = 0
	field_WalCheckSumEnd  = 4

	field_TypeIndexWallInit = 4
	field_TypeIndexWallEnd  = 5

	field_OffsetData_Init_Init = 5
	field_OffsetData_Init_End  = 13

	field_OffsetData_End_Init = 13
	field_OffsetData_End_End  = 21

	field_OffsetWalData_Init_Init = 21
	field_OffsetWalData_Init_End  = 29

	field_OffsetWalData_End_Init = 29
	field_OffsetWalData_End_End  = 37
)

type IndexWallType byte

// 2. Definimos las tres constantes para los tres tipos de Wall
const (
	// Puedes renombrar "TypeA", "TypeB", "TypeC" por nombres que describan su uso
	WallDirectType IndexWallType = 1
	WallModifyType IndexWallType = 2
)

// SetTypeIndexWall escribe el tipo de Wall en la primera posición del index
func SetTypeIndexWall(typeIndex IndexWallType, index []byte) error {

	// Almacenamos el tipo personalizado convirtiéndolo a byte estándar
	index[0] = byte(typeIndex)
	return nil
}

// GetTypeIndexWall lee la primera posición del index y la retorna como IndexWallType
func GetTypeIndexWall(index []byte) (IndexWallType, error) {

	// Leemos el byte y lo convertimos a nuestro tipo personalizado
	return IndexWallType(index[0]), nil
}

var ErrCorruptedData = errors.New("el checksum no coincide")

// SetCheckSum calcula el checksum de data y lo escribe en la sección de checksum de index
func SetCheckSum(index []byte, data []byte) {

	checksum := crc32.Checksum(data, castagnoliTable)

	// Guardamos el checksum en el espacio [0:4] definido por las constantes
	binary.BigEndian.PutUint32(index[field_WalCheckSumInit:field_WalCheckSumEnd], checksum)

}

// GetCheckSum lee el checksum guardado en index y lo compara con el calculado a partir de data
func GetCheckSum(index []byte, data []byte) error {

	// Leer el checksum guardado en el espacio [0:4]
	savedChecksum := binary.BigEndian.Uint32(index[field_WalCheckSumInit:field_WalCheckSumEnd])

	// Calcular el checksum de los datos reales que tenemos
	calculatedChecksum := crc32.Checksum(data, castagnoliTable)

	// Comparar ambos valores
	if savedChecksum != calculatedChecksum {
		return ErrCorruptedData
	}

	return nil
}

// SetOffsetData guarda 'init' y 'end' en sus respectivas posiciones dentro de index

func SetOffsetData(init int64, end int64, index []byte) error {

	// 1. Guardar 'init' en los bytes [4:12]
	binary.BigEndian.PutUint64(index[field_OffsetData_Init_Init:field_OffsetData_Init_End], uint64(init))

	// 2. Guardar 'end' en los bytes [12:20]
	binary.BigEndian.PutUint64(index[field_OffsetData_End_Init:field_OffsetData_End_End], uint64(end))

	return nil
}

// GetOffsetData recupera los valores de 'init' y 'end' desde el buffer index
func GetOffsetData(index []byte) (int64, int64, error) {

	// Leer 'init' de los bytes [4:12]
	init := int64(binary.BigEndian.Uint64(index[field_OffsetData_Init_Init:field_OffsetData_Init_End]))

	// Leer 'end' de los bytes [12:20]
	end := int64(binary.BigEndian.Uint64(index[field_OffsetData_End_Init:field_OffsetData_End_End]))

	return init, end, nil
}

func SetOffsetWalData(init int64, end int64, index []byte) error {

	// 1. Guardar 'init' en los bytes [4:12]
	binary.BigEndian.PutUint64(index[field_OffsetWalData_Init_Init:field_OffsetWalData_Init_End], uint64(init))

	// 2. Guardar 'end' en los bytes [12:20]
	binary.BigEndian.PutUint64(index[field_OffsetWalData_End_Init:field_OffsetWalData_End_End], uint64(end))

	return nil
}

func GetOffsetWalData(index []byte) (int64, int64, error) {

	// Leer 'init' de los bytes [4:12]
	init := int64(binary.BigEndian.Uint64(index[field_OffsetWalData_Init_Init:field_OffsetWalData_Init_End]))

	// Leer 'end' de los bytes [12:20]
	end := int64(binary.BigEndian.Uint64(index[field_OffsetWalData_End_Init:field_OffsetWalData_End_End]))

	return init, end, nil
}

func (pool *dacV3WorkerWriter) processWriteBuffer(j *jobWriter) {

	wallBuffers := pool.wallBuffers[j.bufIdx]

	//Direct escribe solamente un checksum con los datos
	if j.direct {

		// Iteramos sobre cada tarea del lote usando el índice 'i'
		for i := range j.task {

			// 1. Obtenemos un arena y un buffer de índice para ESTA tarea en concreto
			idArena, indexBuf := pool.wallBuffersIndex.addBufferArena()

			// 2. Guardamos el ID en la tarea individual
			j.task[i].idIndexArena = idArena

			// 3. Escribimos los metadatos en el índice temporal (indexBuf)
			SetTypeIndexWall(WallDirectType, indexBuf)

			SetCheckSum(indexBuf, j.task[i].data)

			offsetStart := j.task[i].offset

			offsetEnd := offsetStart + int64(len(j.task[i].data))

			SetOffsetData(offsetStart, offsetEnd, indexBuf)

			// 4. Copiamos este índice temporal a la zona del buffer global asignada a esta tarea
			copy(wallBuffers[j.task[i].indexOffsetStart:j.task[i].indexOffsetEnd], indexBuf)
		}

		return
	}

	//AQui se escribe el indice y los datos en el wall
	// Iteramos sobre cada tarea del lote usando el índice 'i'
	for i := range j.task {

		// 1. Obtenemos un arena y un buffer de índice para ESTA tarea
		idArena, indexBuf := pool.wallBuffersIndex.addBufferArena()

		// 2. Guardamos el ID en la tarea individual
		j.task[i].idIndexArena = idArena

		// 3. Escribimos los metadatos en el índice temporal (indexBuf)
		SetTypeIndexWall(WallModifyType, indexBuf)

		SetCheckSum(indexBuf, j.task[i].data)

		// 4. Calculamos y guardamos los offsets de la página original
		offsetStart := j.task[i].offset
		offsetEnd := offsetStart + int64(len(j.task[i].data))
		SetOffsetData(offsetStart, offsetEnd, indexBuf)

		// 5. Guardamos en el índice en qué parte del WAL están los datos reales
		SetOffsetWalData(j.task[i].dataOffsetStart, j.task[i].dataOffsetEnd, indexBuf)

		// 6. Copiamos este índice temporal a su zona reservada en el buffer global
		copy(wallBuffers[j.task[i].indexOffsetStart:j.task[i].indexOffsetEnd], indexBuf)

		// 7. Finalmente, copiamos los DATOS REALES de la tarea a su zona reservada en el buffer
		copy(wallBuffers[j.task[i].dataOffsetStart:j.task[i].dataOffsetEnd], j.task[i].data)
	}

	return
}
