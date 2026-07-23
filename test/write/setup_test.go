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

dir="/mnt/ramdisk/" interaction="1313" GOMAXPROCS=128 go test ./test/write -run=^TestDacV3_WriteConcurrentWal$ -v -race

*/



