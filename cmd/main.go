package main

import (
	. "dacV3"
	"github.com/cockroachdb/pebble"
	"github.com/dgraph-io/badger/v4"
	"go.etcd.io/bbolt"
	"log"
)

/*
/media/franky/tiuviweb/go/bin/go mod init dacv3Main
/media/franky/tiuviweb/go/bin/go mod tidy

/media/franky/tiuviweb/go/bin/go build -o dacV3Run main.go
chmod +x dacV3Run
./dacV3Run

/media/franky/tiuviweb/go/bin/go build -o dacV3Run main.go && chmod +x dacV3Run && ./dacV3Run

/media/franky/tiuviweb/go/bin/go run main.go
*/

const iteraciones = 2000
const numWorkers = 256 // Cantidad de hilos simultáneos

const textWrite = `Lorem ipsum dolor sit amet, consectetur adipiscing elit. Duis at tellus sit amet massa varius suscipit. Praesent sodales ornare tincidunt. Pellentesque vel augue fermentum, efficitur augue at, dignissim enim. Quisque interdum arcu at lacus consequat, iaculis tincidunt augue condimentum. Sed enim leo, lacinia aliquet consectetur in, pulvinar sit amet est. Mauris pharetra, erat nec pellentesque hendrerit, ante arcu tristique tellus, vitae luctus massa magna vitae sapien. Maecenas ut magna id ex venenatis pellentesque. Suspendisse volutpat dignissim ligula vitae bibendum. Nunc varius semper mattis. Suspendisse eu augue gravida, tristique quam ac, laoreet nunc. Etiam dignissim eget nunc sit amet laoreet. Quisque eu tempus nunc. Integer vel molestie nibh.
Curabitur lobortis, ex eu placerat malesuada, ex augue vehicula erat, ac tincidunt quam arcu et ligula. Maecenas eu enim mauris. Donec nec placerat sapien. Pellentesque leo tellus, mollis at erat in, pellentesque efficitur leo. Maecenas ornare odio eu varius hendrerit. Phasellus vel urna nunc. Nam ultricies libero nunc, a lobortis odio dignissim a. Nunc ligula libero, viverra sed magna in, placerat volutpat metus. Curabitur commodo dolor ante, auctor viverra tortor iaculis a. Morbi lacinia non augue ut ornare. Morbi consectetur, velit et hendrerit tincidunt, risus turpis sodales urna, ut aliquam sapien dolor sed dolor.
Pellentesque facilisis eu turpis sed tempor. In ac magna sed mauris imperdiet pretium. Donec sem erat, mattis ac tincidunt eu, finibus a dolor. Quisque aliquet lectus ut sollicitudin laoreet. Vivamus lacinia, diam sed ultrices dictum, turpis sem ultrices dolor, et pretium augue purus non elit. Donec porttitor vitae ipsum vel sollicitudin. Sed vestibulum dui aliquam odio lacinia pulvinar. Suspendisse potenti. Morbi pellentesque leo suscipit tortor dapibus facilisis. Duis eu mauris justo. Mauris tempor sollicitudin purus eget scelerisque. Aliquam a diam sit amet neque tempus tincidunt. Donec tincidunt risus diam, non consequat lorem gravida vestibulum.
Proin malesuada justo elit, nec congue enim tristique sed. Duis faucibus magna quis hendrerit interdum. Proin consequat varius tincidunt. Integer rutrum, nisl vel condimentum imperdiet, urna tortor rutrum risus, at ullamcorper augue metus et risus. Nunc eget rutrum dui. Etiam commodo lobortis arcu, varius auctor metus maximus at. Maecenas vel turpis accumsan, efficitur nunc ut, mattis risus. Etiam auctor nisi quam, a pretium libero bibendum nec. Praesent at pharetra urna. Integer ligula purus, dapibus eu leo malesuada, condimentum fermentum ligula.
Praesent accumsan lorem ligula, vehicula commodo lectus venenatis in. Praesent vulputate massa quis erat euismod pulvinar vel at mauris. Pellentesque ut nisl tincidunt, euismod erat vitae, hendrerit magna. Cras eros purus, elementum in lectus nec, fermentum laoreet dui. Nunc vitae ipsum luctus, placerat justo eget, vestibulum urna. Aenean bibendum leo sed turpis dapibus, a bibendum eros tempor. Ut ut finibus tortor. Ut pulvinar accumsan imperdiet. In tristique pellentesque massa sed consequat. Proin a accumsan nunc. Fusce dignissim, leo id dictum facilisis, erat orci commodo nibh, id posuere eros ante et urna. Aenean commodo convallis leo vel suscipit. Nullam vel pulvinar tortor. Phasellus in scelerisque tellus.
Donec pulvinar tortor metus, nec molestie urna ultrices quis. Morbi eget volutpat libero. Suspendisse et lacinia turpis, nec dapibus libero. Sed eu tempus purus, at lacinia orci. Mauris eros mauris, vestibulum in fringilla sed, laoreet sollicitudin est. Ut enim massa, hendrerit at turpis in, consequat vestibulum felis. Donec ut enim egestas, tincidunt risus porta, porta neque. Donec interdum, quam eu porta lacinia, est turpis consequat risus, non tincidunt tortor orci sit amet elit. Morbi id condimentum leo, nec hendrerit dui.
Suspendisse molestie tempor enim eget faucibus. Suspendisse potenti. In convallis euismod dignissim. In at libero ac nisl interdum blandit eget a in.`

func main() {

	ramDisk := "/mnt/ramdisk/dacV3.db"
	diskTest := "/mnt/disk/dbDisk.db"

	var diskPath = ""
	if false {
		diskPath = ramDisk
	} else {
		diskPath = diskTest
	}

	multiplierChan := uint32(10)
	config := DacV3Options{
		DacRoute:        diskPath,
		SizeIndexMaster: 4096,              //multiplos de 4096
		MaxReserveSize:  1024 * 1024 * 100, // 100 MB
		SsdNIopsMili:    200,

		NBuffersAvailableIndexSearch:     8,
		NChanAvaibleIndexSearch:          8 * multiplierChan,
		NBuffersAvailableIndexSearchData: MaxSubIndexPerIndex * 8,

		NBuffersAvailableIndex: 23,
		SupportedSizes: []SizeConfig{
			{
				Size:                4096,
				IndexSizeChan:       16 * multiplierChan,
				NBuffersAvaibleData: MaxSubIndexPerIndex * 16,
			},
			{
				Size:                16384,
				IndexSizeChan:       4 * multiplierChan,
				NBuffersAvaibleData: MaxSubIndexPerIndex * 4,
			},
			{
				Size:                32768,
				IndexSizeChan:       2 * multiplierChan,
				NBuffersAvaibleData: MaxSubIndexPerIndex * 2,
			},
			{
				Size:                65536,
				IndexSizeChan:       1 * multiplierChan,
				NBuffersAvaibleData: MaxSubIndexPerIndex,
			},
		},
		NWorkers:  64,
		QueueSize: 16384,
	}

	// Creamos la instancia pasando las opciones (NO se mide)
	db := InitDacV3(config)

	runTestDacV3(db)

	//log.Fatal("exit")

	dbBolt, err := bbolt.Open("/mnt/disk/bolt.db", 0600, nil)
	if err != nil {
		log.Fatal(err)
	}
	defer dbBolt.Close()

	runTestBolt(dbBolt)

	opts := badger.DefaultOptions("/mnt/disk/badgerdb").
		WithSyncWrites(true)

	dbBadger, err := badger.Open(opts)
	if err != nil {
		log.Fatal(err)
	}
	defer dbBadger.Close()

	runTestBadger(dbBadger)

	dbPebble, err := pebble.Open("/mnt/disk/pebble", &pebble.Options{})
	if err != nil {
		log.Fatal(err)
	}
	defer dbPebble.Close()

	runTestPebble(dbPebble)

}
