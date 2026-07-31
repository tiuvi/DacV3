package dacV3

import (
	"encoding/binary"
	"errors"
	"hash/crc32"
)

var castagnoliTable = crc32.MakeTable(crc32.Castagnoli)

const (
	field_WalIndexCheckSumInit = 0
	field_WalIndexCheckSumEnd  = 4

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

	field_Hash_Init = 37
	field_Hash_End  = 69

	field_RelativeOffset_Init = 69
	field_RelativeOffset_End  = 77

	field_DataLen_Init = 77
	field_DataLen_End  = 85

	field_WalCheckSumInit = 85
	field_WalCheckSumEnd  = 89

	field_Sequence_Init = 89
	field_Sequence_End  = 97
)

type IndexWallType byte

// 2. Definimos las tres constantes para los tres tipos de Wall
const (
	// Puedes renombrar "TypeA", "TypeB", "TypeC" por nombres que describan su uso
	WallDirectType IndexWallType = 1
	WallModifyType IndexWallType = 2
)

// SetTypeIndexWall escribe el tipo de Wall en la posición correspondiente (offset 4)
func SetTypeIndexWall(typeIndex IndexWallType, index []byte) {

	index[field_TypeIndexWallInit] = byte(typeIndex)
	return
}

// GetTypeIndexWall lee la posición correspondiente (offset 4) y la retorna como IndexWallType
func GetTypeIndexWall(index []byte) IndexWallType {

	return IndexWallType(index[field_TypeIndexWallInit])
}

var ErrCorruptedData = errors.New("el checksum no coincide")

// SetCheckSum calcula el checksum de data y lo escribe en la sección de checksum de index
func SetCheckSum(index []byte, data []byte) {

	checksum := crc32.Checksum(data, castagnoliTable)

	// Guardamos el checksum en el espacio [0:4] definido por las constantes
	binary.LittleEndian.PutUint32(index[field_WalCheckSumInit:field_WalCheckSumEnd], checksum)

}

// GetCheckSum lee el checksum guardado en index y lo compara con el calculado a partir de data
func GetCheckSum(index []byte, data []byte) error {

	// Leer el checksum guardado en el espacio [0:4]
	savedChecksum := binary.LittleEndian.Uint32(index[field_WalCheckSumInit:field_WalCheckSumEnd])

	// Calcular el checksum de los datos reales que tenemos
	calculatedChecksum := crc32.Checksum(data, castagnoliTable)

	// Comparar ambos valores
	if savedChecksum != calculatedChecksum {
		return ErrCorruptedData
	}

	return nil
}

// SetCheckSumIndex calcula el checksum de los metadatos del índice y lo escribe
func SetCheckSumIndex(index []byte) {

	checksum := crc32.Checksum(index[field_WalIndexCheckSumEnd:], castagnoliTable)

	binary.LittleEndian.PutUint32(index[field_WalIndexCheckSumInit:field_WalIndexCheckSumEnd], checksum)
}

// GetCheckSumIndex lee el checksum del índice y verifica su integridad
func GetCheckSumIndex(index []byte) error {

	savedChecksum := binary.LittleEndian.Uint32(index[field_WalIndexCheckSumInit:field_WalIndexCheckSumEnd])

	calculatedChecksum := crc32.Checksum(index[field_WalIndexCheckSumEnd:], castagnoliTable)

	if savedChecksum != calculatedChecksum {
		return ErrCorruptedData
	}

	return nil
}

// SetOffsetData guarda 'init' y 'end' en sus respectivas posiciones dentro de index

func SetOffsetData(init int64, end int64, index []byte) error {

	// 1. Guardar 'init' en los bytes [4:12]
	binary.LittleEndian.PutUint64(index[field_OffsetData_Init_Init:field_OffsetData_Init_End], uint64(init))

	// 2. Guardar 'end' en los bytes [12:20]
	binary.LittleEndian.PutUint64(index[field_OffsetData_End_Init:field_OffsetData_End_End], uint64(end))

	return nil
}

// GetOffsetData recupera los valores de 'init' y 'end' desde el buffer index
func GetOffsetData(index []byte) (int64, int64, error) {

	// Leer 'init' de los bytes [4:12]
	init := int64(binary.LittleEndian.Uint64(index[field_OffsetData_Init_Init:field_OffsetData_Init_End]))

	// Leer 'end' de los bytes [12:20]
	end := int64(binary.LittleEndian.Uint64(index[field_OffsetData_End_Init:field_OffsetData_End_End]))

	return init, end, nil
}

func SetOffsetWalData(init int64, end int64, index []byte) error {

	// 1. Guardar 'init' en los bytes [4:12]
	binary.LittleEndian.PutUint64(index[field_OffsetWalData_Init_Init:field_OffsetWalData_Init_End], uint64(init))

	// 2. Guardar 'end' en los bytes [12:20]
	binary.LittleEndian.PutUint64(index[field_OffsetWalData_End_Init:field_OffsetWalData_End_End], uint64(end))

	return nil
}

func GetOffsetWalData(index []byte) (int64, int64, error) {

	// Leer 'init' de los bytes [4:12]
	init := int64(binary.LittleEndian.Uint64(index[field_OffsetWalData_Init_Init:field_OffsetWalData_Init_End]))

	// Leer 'end' de los bytes [12:20]
	end := int64(binary.LittleEndian.Uint64(index[field_OffsetWalData_End_Init:field_OffsetWalData_End_End]))

	return init, end, nil
}

func SetHashWallData(hash [32]byte, index []byte) {
	copy(index[field_Hash_Init:field_Hash_End], hash[:])
	return
}

func GetHashWallData(index []byte) [32]byte {
	var hash [32]byte
	copy(hash[:], index[field_Hash_Init:field_Hash_End])
	return hash
}

func SetRelativeOffsetWall(offset int64, index []byte) {
	binary.LittleEndian.PutUint64(index[field_RelativeOffset_Init:field_RelativeOffset_End], uint64(offset))
}

func GetRelativeOffsetWall(index []byte) int64 {
	return int64(binary.LittleEndian.Uint64(index[field_RelativeOffset_Init:field_RelativeOffset_End]))
}

func SetDataLenWall(dataLen int64, index []byte) {
	binary.LittleEndian.PutUint64(index[field_DataLen_Init:field_DataLen_End], uint64(dataLen))
}

func GetDataLenWall(index []byte) int64 {
	return int64(binary.LittleEndian.Uint64(index[field_DataLen_Init:field_DataLen_End]))
}

// SetSequence escribe el número de secuencia en el índice WAL
func SetSequence(sequence uint64, index []byte) {
	binary.LittleEndian.PutUint64(index[field_Sequence_Init:field_Sequence_End], sequence)
}

// GetSequence lee el número de secuencia almacenado en el índice WAL
func GetSequence(index []byte) uint64 {
	return binary.LittleEndian.Uint64(index[field_Sequence_Init:field_Sequence_End])
}

func (pool *dacV3WorkerWriter) processWriteBuffer(j *jobWriter) {

	
	walBuffersTotal := pool.walBuffersTotal[j.bufIdx]

	//Direct escribe solamente un checksum con los datos
	if j.direct {

		// Iteramos sobre cada tarea del lote usando el índice 'i'
		for i := range j.task {

			indexView := walBuffersTotal[j.task[i].indexOffsetStart:j.task[i].indexOffsetEnd]

			// 2. Escribimos los metadatos DIRECTAMENTE en el buffer global usando la vista
			SetTypeIndexWall(WallDirectType, indexView)

			SetCheckSum(indexView, j.task[i].data)

			offsetStart := j.task[i].offset
			offsetEnd := offsetStart + int64(len(j.task[i].data))

			SetOffsetData(offsetStart, offsetEnd, indexView)

			SetHashWallData(j.task[i].hash, indexView)

			SetRelativeOffsetWall(j.task[i].relativeOffset, indexView)

			SetDataLenWall(j.task[i].dataLen, indexView)

			SetSequence(pool.walSequence , indexView)

			SetCheckSumIndex(indexView)
		}

		return
	}

	//AQui se escribe el indice y los datos en el wall
	// Iteramos sobre cada tarea del lote usando el índice 'i'
	for i := range j.task {

		t := &j.task[i] // Usamos un puntero para limpiar la sintaxis

		// 1. Creamos la vista directa para el ÍNDICE (metadatos) en el buffer global
		indexView := walBuffersTotal[t.indexOffsetStart:t.indexOffsetEnd]

		// 2. Escribimos los metadatos DIRECTAMENTE en la vista global (Zero-Copy para el índice)
		SetTypeIndexWall(WallModifyType, indexView)
		SetCheckSum(indexView, t.data)

		// 3. Calculamos y guardamos los offsets de la página original en el índice
		offsetStart := t.offset
		offsetEnd := offsetStart + int64(len(t.data))
		SetOffsetData(offsetStart, offsetEnd, indexView)

		// 4. Guardamos en el índice en qué parte del WAL están los datos reales
		SetOffsetWalData(t.dataOffsetStart, t.dataOffsetEnd, indexView)

		// 4.5. Guardamos el hash de los datos en el índice
		SetHashWallData(t.hash, indexView)

		SetRelativeOffsetWall(t.relativeOffset, indexView)
		
		SetDataLenWall(t.dataLen, indexView)
		
		SetSequence(pool.walSequence , indexView)

		SetCheckSumIndex(indexView)

		// 5. Copiamos los DATOS REALES de la tarea a su zona en el buffer global
		// (Esta copia es necesaria y ya está optimizada, va directa al destino final)
		copy(walBuffersTotal[t.dataOffsetStart:t.dataOffsetEnd], t.data)

	}

	return
}
