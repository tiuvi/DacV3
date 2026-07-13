package main

import . "dacV3"

/*
/media/franky/tiuviweb/go/bin/go mod init dacv3Main
/media/franky/tiuviweb/go/bin/go mod tidy

/media/franky/tiuviweb/go/bin/go build -o dacV3Run main.go
chmod +x dacV3Run
./dacV3Run

/media/franky/tiuviweb/go/bin/go build -o dacV3Run main.go && chmod +x dacV3Run && ./dacV3Run
*/


func main() {

	// Definimos las opciones de forma muy visual y explícita
	config := DacV3Options{
		DacRoute:        "/mnt/ramdisk/dacV3.db",
		SizeIndexMaster: 4096,              //multiplos de 4096
		MaxReserveSize:  1024 * 1024 * 100, // 100 MB
		SsdNIopsMili:    50,

		NBuffersAvailableIndexSearch:     8,
		NChanAvaibleIndexSearch:          8,
		NBuffersAvailableIndexSearchData: MaxSubIndexPerIndex * 8,

		NBuffersAvailableIndex: 23,
		SupportedSizes: []SizeConfig{
			{
				Size:                4096,
				IndexSizeChan:       16,
				NBuffersAvaibleData: MaxSubIndexPerIndex * 16,
			},
			{
				Size:                16384,
				IndexSizeChan:       4,
				NBuffersAvaibleData: MaxSubIndexPerIndex * 4,
			},
			{
				Size:                32768,
				IndexSizeChan:       2,
				NBuffersAvaibleData: MaxSubIndexPerIndex * 2,
			},
			{
				Size:                65536,
				IndexSizeChan:       1,
				NBuffersAvaibleData: MaxSubIndexPerIndex,
			},
		},
		NWorkers:  8,
		QueueSize: 1024,
	}

	// Creamos la instancia pasando las opciones
	db := InitDacV3(config)




}
