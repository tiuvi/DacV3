package test

/*
/media/franky/tiuviweb/go/bin/go mod init dacv3Main
/media/franky/tiuviweb/go/bin/go mod tidy

/media/franky/tiuviweb/go/bin/go build -o dacV3Run main.go
chmod +x dacV3Run
./dacV3Run

/media/franky/tiuviweb/go/bin/go build -o dacV3Run main.go && chmod +x dacV3Run && ./dacV3Run

/media/franky/tiuviweb/go/bin/go run main.go


para test

# RAMDisk (1 GB)
sudo mkdir -p /mnt/ramdisk
sudo mount -t tmpfs -o size=1G tmpfs /mnt/ramdisk
sudo chown $USER:$USER /mnt/ramdisk


Verifica:

df -h /mnt/ramdisk

Desmontar:

sudo umount /mnt/ramdisk

# Archivo en disco (1 GB)
sudo mkdir -p /mnt/disk
sudo fallocate -l 1G /mnt/disk/dbDisk.db
sudo chown $USER:$USER /mnt/disk/dbDisk.db


export PATH=$PATH:/media/franky/tiuviweb/go/bin

dir="/mnt/disk" interaction="2000" GOMAXPROCS=128 go test ./test/bench -bench=Pebble -benchtime=2000x

dir="/mnt/disk/" interaction="2000" GOMAXPROCS=128 go test ./test/bench -bench=Badger -benchtime=2000x

dir="/mnt/disk/" interaction="2000" GOMAXPROCS=128 go test ./test/bench -bench=Bolt -benchtime=2000x

dir="/mnt/disk/" interaction="2000" GOMAXPROCS=128 go test ./test/bench -bench=DacV3 -benchtime=2000x

dir="/mnt/ramdisk/" interaction="2000" GOMAXPROCS=128 go test ./test/bench -bench=DacV3 -benchtime=2000x

dir="/mnt/ramdisk/" interaction="10" GOMAXPROCS=128 go test ./test/bench -bench=DacV3 -benchtime=10x

dir="/mnt/ramdisk/" interaction="10" GOMAXPROCS=128 go test ./test/bench -bench=^BenchmarkDacV3_WriteConcurrentWal$ -benchtime=10x

dir="/mnt/ramdisk/" interaction="10" GOMAXPROCS=128 go test ./test/bench -bench=^BenchmarkDacV3_WriteConcurrent$ -benchtime=10x

//Test para comprobar la creacion de indices y nuevas paginas
dir="/mnt/ramdisk/" interaction="1313" GOMAXPROCS=128 go test ./test/write -run=^TestDacV3_WriteConcurrentWal$ -v -race


//Test para comproba el crecimiento de un archivo hasta 16k
dir="/mnt/ramdisk/" interaction="100" go test ./test/write -run=^TestDacV3_SingleGrowingPage_LineByLine$ -v



Crash cuando se reservo un indice en memoria, deben ser retirados en initpages
clear && crashType=2 dir="/mnt/ramdisk/" interaction="100" go test ./test/write -run=^TestDacV3_CrashEnergy$ -v

Antes de cambiar los indices cuando el archivo aumenta de tamaño
clear && crashType=3 dir="/mnt/ramdisk/" interaction="100" go test ./test/write -run=^TestDacV3_CrashEnergy$ -v

Crash cuando se va hacer el intercambio de indices
clear && crashType=4 dir="/mnt/ramdisk/" interaction="100" go test ./test/write -run=^TestDacV3_CrashEnergy$ -v

Crash despues de a ver escrito en el wal el checksum, pero solo se escriben los datos a medias.
clear && crashType=5 dir="/mnt/ramdisk/" interaction="100" go test ./test/write -run=^TestDacV3_CrashEnergy$ -v

Crash escribiendo el wall a medias
clear && crashType=9 dir="/mnt/ramdisk/" interaction="100" go test ./test/write -run=^TestDacV3_CrashEnergy$ -v

escribiendo un indice de wal incorrecto
clear && crashType=23 dir="/mnt/ramdisk/" interaction="100" go test ./test/write -run=^TestDacV3_CrashEnergy$ -v



Crecimiento de archivo hasta 64k
clear && dir="/mnt/ramdisk/" interaction="1000" go test ./test/write -run=^TestDacV3_SingleGrowingPage_LineByLine$ -v

Cuando se cambia a una pagina superior deja corrupto el indice nuevo
clear && crashType=10 crashTypeLine=60 interaction="1000" dir="/mnt/ramdisk/" go test ./test/write -run=^TestDacV3_CrashEnergy$ -v

Cuando se cambia a una pagina superior deja corrupto el indice viejo
clear && crashType=11 crashTypeLine=60 interaction="1000" dir="/mnt/ramdisk/" go test ./test/write -run=^TestDacV3_CrashEnergy$ -v

corrupcion durante la escritura
clear && crashType=12 crashTypeLine=60 interaction="1000" dir="/mnt/ramdisk/" go test ./test/write -run=^TestDacV3_CrashEnergy$ -v

corrucion en el wall
clear && crashType=13 crashTypeLine=60 interaction="1000" dir="/mnt/ramdisk/" go test ./test/write -run=^TestDacV3_CrashEnergy$ -v

clear && crashType=21 crashTypeLine=60 interaction="1000" dir="/mnt/ramdisk/" go test ./test/write -run=^TestDacV3_CrashEnergy$ -v

Corrupcion despues de escribir
clear && crashType=14 crashTypeLine=60 interaction="1000" dir="/mnt/ramdisk/" go test ./test/write -run=^TestDacV3_CrashEnergy$ -v

Corrupcion en el indice despues de que esten bien los datos
clear && crashType=15 crashTypeLine=60 interaction="1000" dir="/mnt/ramdisk/" go test ./test/write -run=^TestDacV3_CrashEnergy$ -v






Corrupcion en la escritura despues de escribir en el wal, cuando se escribe en la ubicacion de la pagina
clear && crashType=16 crashTypeLine=10 interaction="1000" dir="/mnt/ramdisk/" go test ./test/write -run=^TestDacV3_CrashEnergy$ -v
clear && crashType=16 crashTypeLine=65 interaction="1000" dir="/mnt/ramdisk/" go test ./test/write -run=^TestDacV3_CrashEnergy$ -v
clear && crashType=16 crashTypeLine=500 interaction="1000" dir="/mnt/ramdisk/" go test ./test/write -run=^TestDacV3_CrashEnergy$ -v


Corrupcion en la escritura cuando se esta escribiendo en el wall
clear && crashType=17 crashTypeLine=10 interaction="1000" dir="/mnt/ramdisk/" go test ./test/write -run=^TestDacV3_CrashEnergy$ -v

clear && crashType=22 crashTypeLine=10 interaction="1000" dir="/mnt/ramdisk/" go test ./test/write -run=^TestDacV3_CrashEnergy$ -v



Corrupcion en la escritura despues de escribirse
clear && crashType=18 crashTypeLine=10 interaction="1000" dir="/mnt/ramdisk/" go test ./test/write -run=^TestDacV3_CrashEnergy$ -v

Corrupcion del indice despues de escribir en el wal
clear && crashType=19 crashTypeLine=10 interaction="1000" dir="/mnt/ramdisk/" go test ./test/write -run=^TestDacV3_CrashEnergy$ -v




clear && crashType=6 crashTypeLine=500 interaction="1000" dir="/mnt/ramdisk/" go test ./test/write -run=^TestDacV3_CrashEnergy$ -v

clear && go test ./test/write -count=1 -run=^TestAllCrash$ -timeout 2h -v

/media/franky/tiuviweb/go/bin go test ./test/write -run TestAllCrash -timeout 2h -v



*/
