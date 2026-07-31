package dacV3

import (
	"bytes"
	"sort"
)

type WalUpdateEntry struct {
	Hash           [32]byte // O []byte si prefieres slices dinamicos
	OffsetRelative int64
	Offset         int64
	Data           []byte
}

type WalCorruptEntry struct {
	secuence       uint64
	Hash           [32]byte // O []byte si prefieres slices dinamicos
	OffsetRelative int64
	dataLen        int64
}

func readWalBuffers(sfDacV3 *DacV3, walsBuffer [][]byte, numOfBuffersWal, walLenControlBlock, walLenIndexBytes, walLenTotalBytes int64) (pendingUpdates []WalUpdateEntry, pendingCorrupt []WalCorruptEntry, walSequence uint64) {

	pendingUpdates = make([]WalUpdateEntry, 0, int64(sfDacV3.opts.SsdNIopsMili)*numOfBuffersWal)

	pendingCorrupt = make([]WalCorruptEntry, 0, int64(sfDacV3.opts.SsdNIopsMili)*numOfBuffersWal)

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
			seq := bufWalControl(buf).GetSequence()
			if seq == 0 {
				continue
			}

			infos = append(infos, walBufferInfo{index: i, seq: seq})

			//Copiamos el buffer a su wal correspondiente
			copy(walsBuffer[i], buf)
		}

		if len(infos) == 0 {
			return nil, nil, 1
		}

		//Ordenamos los wal por secuencia
		sort.Slice(infos, func(i, j int) bool {
			return infos[i].seq < infos[j].seq
		})

		for _, info := range infos {

			buf := walsBuffer[info.index]

			println("secuencia wal", info.seq)
			if !bufWalControl(buf).IsEmpty() {

				for indexOffset := walLenControlBlock; indexOffset < walLenIndexBytes; indexOffset += int64(BufferAlignSize) {

					indexView := buf[indexOffset : indexOffset+int64(BufferAlignSize)]

					walType := GetTypeIndexWall(indexView)

					//Si el indice es 0 es que aqui no hay datos de indices ya.
					if walType == 0 {
						continue
					}

					err := GetCheckSumIndex(indexView)
					if err != nil {
						println("CORRUPCION EN UN INDICE DE WAL, PERDIDA DE DATOS INEVITABLE.")
						continue
					}

					
					seqData := GetSequence(indexView)
					if info.seq != seqData {
					
						continue
					}

					if walType == WallDirectType {

						offsetStart, offsetEnd, _ := GetOffsetData(indexView)

						dataDirect := MakeAlignedBlock(int(offsetEnd - offsetStart))

						sfDacV3.ReadAt(dataDirect, offsetStart)

						err := GetCheckSum(indexView, dataDirect)

						//Si el checksum falla , borramos los datos en el offset original.
						if err != nil {

							hash := GetHashWallData(indexView)

							offsetRelative := GetRelativeOffsetWall(indexView)

							dataLen := GetDataLenWall(indexView)

							pendingCorrupt = append(pendingCorrupt, WalCorruptEntry{
								secuence:       info.seq,
								Hash:           hash,
								OffsetRelative: offsetRelative,
								dataLen:        dataLen,
							})

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

							hash := GetHashWallData(indexView)

							offsetRelative := GetRelativeOffsetWall(indexView)

							dataLen := GetDataLenWall(indexView)

							pendingCorrupt = append(pendingCorrupt, WalCorruptEntry{
								secuence:       info.seq,
								Hash:           hash,
								OffsetRelative: offsetRelative,
								dataLen:        dataLen,
							})
							continue
						}

						//Si los datos son correctos , escribimos los datos directamente sin verificar si ya se escribieron antes
						offsetStart, _, _ := GetOffsetData(indexView)

						hash := GetHashWallData(indexView)

						//Esta escritura la usa el indexmaster
						if hash == hashZero {

							//ay que saber donde se escribe, para actualizar el indice tambien
							sfDacV3.WriteAt(dataWal, offsetStart)

						} else {

							offsetRelative := GetRelativeOffsetWall(indexView)

							dataLen := GetDataLenWall(indexView)

							offsetEnPagina := offsetRelative % int64(len(dataWal))

							newDataWal := bytes.Clone(dataWal[offsetEnPagina : offsetEnPagina+dataLen])

							println("readWalBuffers: ", "sec:" , info.seq ,gettLastLine(string(newDataWal)), "offset:", offsetRelative)
							//println("readWalBuffers datawal: ", gettLastLine(string(dataWal)), "offset:", offsetRelative)

							sfDacV3.WriteAt(dataWal, offsetStart)

							pendingUpdates = append(pendingUpdates, WalUpdateEntry{
								Hash:           hash,
								OffsetRelative: offsetRelative,
								Offset:         offsetStart,
								Data:           newDataWal,
							})

						}

					}
				}

				if info.seq >= walSequence {
					walSequence = info.seq + 1
				}
			}
		}
	}

	return pendingUpdates, pendingCorrupt, walSequence
}

func startHandleWallBuffer(sfDacV3 *DacV3) (pendingUpdates []WalUpdateEntry, pendingCorrupt []WalCorruptEntry) {

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

	//Esta parte va ver que moverla al final, seguro que NewWorkerPool pasa estas variables a globales
	pendingUpdates, pendingCorrupt, walSequence := readWalBuffers(sfDacV3, walsBuffer, int64(numOfBuffersWal), walLenControlBlock, walLenIndexBytes, walLenTotalBytes)
	if walSequence == 0 {
		walSequence = 1
	}

	sfDacV3.NewWorkerPool(sfDacV3.opts.NWorkers,
		sfDacV3.opts.QueueSize,
		walSequence,
		walLenIndexBytes,
		numOfBuffersWal,
		walLenTotalBytes,
		walsBuffer)

	//leer wall y repartir datos.
	return pendingUpdates, pendingCorrupt
}
