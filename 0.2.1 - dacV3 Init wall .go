package dacV3

import (
	"encoding/binary"
	"sort"
)

func readWalBuffers(sfDacV3 *DacV3, walsBuffer [][]byte, numOfBuffersWal, walLenControlBlock, walLenIndexBytes, walLenTotalBytes int64) (walSequence uint64) {

	fileSize := sfDacV3.len.Load()
	if fileSize > 0 {

		walSumBuffersSize := int64(numOfBuffersWal) * walLenTotalBytes

		allWalData := MakeAlignedBlock(int(walSumBuffersSize))

		sfDacV3.ReadAt(allWalData, 0)

		type walBufferInfo struct {
			index int
			seq   uint64
		}

		var infos []walBufferInfo

		for i := 0; i < int(numOfBuffersWal); i++ {

			start := int64(i) * walLenTotalBytes

			end := start + walLenTotalBytes

			buf := allWalData[start:end]

			//Leemos la secuencia del wal
			seq := binary.BigEndian.Uint64(buf[0:8])

			infos = append(infos, walBufferInfo{index: i, seq: seq})

			//Copiamos el buffer a su wal correspondiente
			copy(walsBuffer[i], buf)
		}

		//Ordenamos los wal por secuencia
		sort.Slice(infos, func(i, j int) bool {
			return infos[i].seq < infos[j].seq
		})

		for _, info := range infos {

			buf := walsBuffer[info.index]

			if info.seq > 0 || buf[walLenControlBlock] != 0 {

				for indexOffset := walLenControlBlock; indexOffset < walLenIndexBytes; indexOffset += int64(BufferAlignSize) {

					indexView := buf[indexOffset : indexOffset+int64(BufferAlignSize)]

					walType, _ := GetTypeIndexWall(indexView)

					//Si el indice es 0 es que aqui no hay datos de indices ya.
					if walType == 0 {
						continue
					}

					if walType == WallDirectType {

						offsetStart, offsetEnd, _ := GetOffsetData(indexView)

						dataDirect := MakeAlignedBlock(int(offsetEnd - offsetStart))

						sfDacV3.ReadAt(dataDirect, offsetStart)

						err := GetCheckSum(indexView, dataDirect)

						//Si el checksum falla , borramos los datos en el offset original.
						if err != nil {

							println("readWalBuffers - GetCheckSum - WallDirectType - El wal tiene datos corruptos")

							clear(dataDirect)

							sfDacV3.WriteAt(dataDirect, offsetStart)

							continue
						}

						//Si no hay error no hacemos nada , los datos son correctos
						continue
					}

					if walType == WallModifyType {

						//Si es una modificacion primero verificamos el checksum del wal
						offsetStartWalData, offsetEndWalData, _ := GetOffsetWalData(indexView)

						dataWal := buf[offsetStartWalData:offsetEndWalData]

						err := GetCheckSum(indexView, dataWal)
						if err != nil {
							println("readWalBuffers - GetCheckSum - WallModifyType - El wal tiene datos corruptos")
							continue
						}

						//Si los datos son correctos , escribimos los datos directamente sin verificar si ya se escribieron antes
						offsetStart, _, _ := GetOffsetData(indexView)

						sfDacV3.WriteAt(dataWal, offsetStart)

					}
				}

				if info.seq >= walSequence {
					walSequence = info.seq + 1
				}
			}
		}
	}

	return walSequence
}

func startHandleWallBuffer(sfDacV3 *DacV3) {

	walLenControlBlock := int64(BufferAlignSize)

	//Numero de operaciones por milisegundo, por el total de los buffers por lo que ocupa cada pagina de indice
	walLenIndexBytes := int64(sfDacV3.opts.SsdNIopsMili*BufferAlignSize) + walLenControlBlock

	maxPageSize := sfDacV3.indexMaster.blockMaxSize.pageSize

	sizeData := uint32(sfDacV3.opts.SsdNIopsMili) * uint32(maxPageSize)

	walLenTotalBytes := walLenIndexBytes + int64(sizeData)

	numOfBuffersWal := 3

	walsBuffer := make([][]byte, numOfBuffersWal)

	walBufferArena := newBufferArena(uint32(numOfBuffersWal), walLenTotalBytes)

	for i := range numOfBuffersWal {

		_, buf := walBufferArena.addBufferArena()

		walsBuffer[i] = buf
	}

	//Añadir aqui lectura de wal

	walSequence := readWalBuffers(sfDacV3, walsBuffer, int64(numOfBuffersWal), walLenControlBlock, walLenIndexBytes, walLenTotalBytes)

	sfDacV3.NewWorkerPool(sfDacV3.opts.NWorkers,
		sfDacV3.opts.QueueSize,
		walSequence,
		walLenIndexBytes,
		numOfBuffersWal,
		walLenTotalBytes,
		walsBuffer)

	//leer wall y repartir datos.

}
