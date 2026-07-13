package dacV3

import (
	"errors"
	"sync/atomic"
)

/*
/media/franky/tiuviweb/go/bin/go mod tidy

/media/franky/tiuviweb/go/bin/go build -o dacV3Run main.go
chmod +x dacV3Run
./dacV3Run

/media/franky/tiuviweb/go/bin/go run main.go


/media/franky/tiuviweb/go/bin/go mod init dacv3
/media/franky/tiuviweb/go/bin/go get golang.org/x/sys/unix
/media/franky/tiuviweb/go/bin/go run main.go

Inicio de doc V3 y mecanismos de recuperación

1° Lectura de WAL
Si el tipo de índice es 0 se descarta.

Si el tipo de índice es directo se pide los datos en su offset original y se compara
con el checksum; si no coincide se abortan los datos.

Si el tipo de índice es modify se pide los datos que hay en el WAL; si coincide con el checksum
se escriben los datos en su origen.

La lectura del WAL sigue orden de secuencia.


2° Lectura e iniciación del almacenamiento global
Primero se lee el mapa de almacenamiento global y se lee en las posiciones ocupadas; en
caso de no estar ocupada una posición serían datos huérfanos.


3° Lectura e iniciación de índices

Verificar secuencia superior en el índice.

Verificar el checksum.

4º Lectura e iniciacion de pages

Comprobar lista de activados con todos los subindices de paginas, si en la lista se activo
pero el subindice esta vacio borrarlo de la lista de activados. (Significa que se
reservo un espacio en ese indice pero al final no se escribio)

verificar la version de los subindices de paginas el superior gana


Creacion de indexMaster y gestion del espacio

index master es un mapa de bits donde cada bit respresenta el bloque mas pequeño de la
configuracion de bloques

Su unica funcion newIndexs crea indices y reserva el tamaño en el mapa de bits

En caso de falta de espacio tambien amplia el tamaño total del archivo.

Escritura en orden:
	-primero se guardan los indices directos en el disco
	-Despues se actualiza el mapa de bit y se guarda en el disco

recuperacion
	-Cada vez que se inicia hay que verificar el mapa de bits y leer en los bloques
	ocupados.


APERTURA DE ARCHIVOS Y GESTION DEL TAMAÑO DEL ARCHIVO

	Apertura de archivo con cap 0

	Abre una pagina minima por ejemplo de tamaño 4096, si el tamaño aumenta
	entonces se mueve todo el contenido a un tamaño superior por ejemplo 16384

	Esto ocurre hasta llegar al tamaño maximo de pagina de por ejemplo 65536

	Cuando llega al tamaño maximo esa pagina hay que moverla a un indice indexado.
	En la pagina vez de datos tiene hash que apuntan a indices indexados mas la posicion
	exacta de la pagina (hash indice indexado + subindice)

	En los indices indexados encontramos los datos

	¿Que es es un indice indexado?
	significa que en vez de indexar las paginas se indexa el indice y se accede mediante
	un mapa indice -> id y despues sabiendo la posicion del id en ese indice


APERTURAS DE ARCHIVOS POR NOMBRE O POR HASH

Primer caso apertura de pagina
La ruta se pasa por un hash si existe se abre la pagina si no existe se crea

	func openRoute(route [128]byte)

Segundo caso apertura con hash,
Se accede directamente a la pagina y si no existe se crea con el hash dado

	func openWithHash(hash [32]byte)

Tercer caso las mismas funciones peri si no existe ya el hash devuelve error

	func openRouteIfNotExist(route [128]byte)

	func openWithHashIfNotExist(hash [32]byte)

ESCRITURAS INDICES

	Los indices tienen el doble de su tamaño para hacer pingpong entre uno
	Los indices siempre permanecen en memoria y su buffer se reutiliza nunca se elimina

	funcionalidades
		Crear un archivo
			funcion -> WritePageDirect
		Eliminar un archivo, escribir datos vacios.
			funcion -> WritePageDirect


ESCRITURAS DATOS



Primer caso la escritura escritura en un offset superior al tamaño del archivo , necesitando
una nueva pagina.

	-Primero se escriben todos los datos en una pagina nueva
	-se boquea solamente en memoria usando field_IndexKeptInit
		funcion -> WritePageDirect


	2º Se actualizan los dos indices en disco a la vez usando el wall
		funcion -> WritePageWall

	Recuperacion:
	Si esta escrito en el wal, se vuelven a escribir los indices y se recupera el nuevo archivo,
	si los indices no se han escrito la pagina se queda desbloqueada pero con datos antiguos que
	abria que limpiar.



Segundo caso escritura en un offset superior al tamaño del archivo pero sin ser mas grande que el archivo total

	-Primero se escribe en el wal , la escritura offset y un checksum con cr32
	-Segundo se escriben los datos directos en el archivo
		funcion -> WritePageDirect

	-tercero se actualiza el indice con el tamaño del subindice directamente subiendo la secuencia

	Recuperacion:
		-Si existe el wall se compara los datos que se han editado con el checksum en caso de que no coincida
		los datos se borran.
		-Si el indice no se actualiza quedan datos huerfanos que al volver ser escritos se actualizarian
		-Si no existe el wall se busca los datos con el checksum y si no coincide se borran.

	ADICIONAL:
		Esta escritura permite archivos de grandes puedan ser escritos directamente los datos sin ser reubicados


Tercer caso escritura en un offset inferior al tamaño

	-primero se escribe en un wall los datos completos y donde van
	-segundo se escribe en la pagina donde corresponda.
		funcion -> WritePageWall

	Recuperacion:
		-Si existen los wall se encolan en orden


LECTURA DE DATOS

	Primer caso si la pagina no ha sido abierta

		-Primero se busca el hash
		-Se pide un bufer
		-Se sincroniza los datos con el disco

	Segundo caso si la pagina ya ha sido abierta

		-Se responde directamente desde el buffer

	Tercer caso lecturas de archivos grandes

		- por defeninir



*/

/*
https://go.dev/play/

Calculo de tamaño page 32
Calculo de tamaño index 32

package main

import (
	"sync"
	"unsafe"
)

type Index struct {
	mu               sync.Mutex
	isBuffering      bool
	idLocationBuffer uint32
	idLocationIndex  uint32
	offset           int64
}

type Page struct {

	mu sync.Mutex

	sizeFile uint64
	isBuffering bool
	idBuffer uint32
	idIndex uint32
	idSubIndex uint8
}


func main() {

	p := Page{}

	println("El struct ocupa:", unsafe.Sizeof(p), "bytes")

	idx := Index{}

	println("Tamaño de Index:", unsafe.Sizeof(idx), "bytes")
}


*/

/*
Calculo del mapa 78 bytes
package main

import (
	"encoding/binary"
	"runtime"
)

func main() {
	var m1, m2 runtime.MemStats

	runtime.GC()
	runtime.ReadMemStats(&m1)

	const totalEntradas = 10000

	// ¡AQUÍ ESTÁ LA MAGIA! Le decimos que reserve espacio para 10,000.
	files := make(map[[32]byte]int, totalEntradas)

	for i := 0; i < totalEntradas; i++ {
		var key [32]byte
		binary.BigEndian.PutUint64(key[0:8], uint64(i))
		files[key] = i
	}

	runtime.ReadMemStats(&m2)

	totalAlloc := m2.Alloc - m1.Alloc
	println("Bytes totales asignados:", totalAlloc)
	println("Promedio por entrada (Preasignado):", totalAlloc / totalEntradas, "bytes")
}



para test

sudo mkdir /mnt/ramdisk
sudo mount -t tmpfs -o size=1G tmpfs /mnt/ramdisk
sudo chown $USER:$USER /mnt/ramdisk

Verifica:

df -h /mnt/ramdisk

Desmontar:

sudo umount /mnt/ramdisk

*/

type WalZeroCopy struct {
	id       uint64
	offset   uint64
	size     uint64
	sequence atomic.Uint64
}

type WalAppend struct {
	id       uint64
	offset   uint64
	size     uint64
	sequence atomic.Uint64
}

/*
4096 * 51 208896

4096
16384
65536
262144
1048576
*/

var errNoMoreIndex = errors.New("no hay mas indices")

func (sfDacV3 *dacV3) initDacV3() {

}

var dacV3File *dacV3

/*
Optimizar a partir de datos de entrada como
megabytes de escritura del ssd e iops, la operacion es una division
12 KB	≈ 65 000 IOPS
16 KB	≈ 51 200 IOPS
64 KB	≈ 12 800 IOPS

ESto se traduce en
65 operaciones de 12kb por milesima de segundo
12.8 operaciones de 64kb por milesima de segundo

Tamaño para los indices maximo seria
64*4092 = 261888
Tamaño para los datos maximo seria
64000	* 12.8 = 819200
tamaño total
1081088


*/

type DacV3Options struct {
	dacRoute string
	sizeIndexMaster        int
	MaxReserveSize         int64
	SsdNIopsMili           uint32
	NBuffersAvailableIndex uint32

	NChanAvaibleIndexSearch      uint32
	NBuffersAvailableIndexSearch uint32

	NBuffersAvailableIndexSearchData uint32
	SupportedSizes                   []SizeConfig
	NWorkers                         int
	QueueSize                        int
}

/*
const SsdNIopsMili = 2000

const totalWalIndexBuffer = (SsdNIopsMili * BufferAlignSize)

const totalWalDataBuffer = (SsdNIopsMili * 65536)

const totalIndexSumData = (totalWalIndexBuffer + totalWalDataBuffer) * 3

const totalInMb = totalIndexSumData / int64(Megabyte)
*/

const totalClusterPages = 65536 / 32
const totalBytesPerClusterPage = 65536 * totalClusterPages
const totalBytesPerClusterPageMb = totalBytesPerClusterPage / int64(Megabyte)

const totalIndexbytes = Terabyte / (maxSubIndexPerIndex * 4096)
const totalIndexMb = (totalIndexbytes * BufferAlignSize) / Gigabyte

func main() {

	// Definimos las opciones de forma muy visual y explícita
	config := DacV3Options{
		dacRoute:"/mnt/ramdisk",
		sizeIndexMaster: 4096,              //multiplos de 4096
		MaxReserveSize:  1024 * 1024 * 100, // 100 MB
		SsdNIopsMili:    50,

		NBuffersAvailableIndexSearch:     8,
		NChanAvaibleIndexSearch:          8,
		NBuffersAvailableIndexSearchData: maxSubIndexPerIndex * 8,

		NBuffersAvailableIndex: 23,
		SupportedSizes: []SizeConfig{
			{
				Size:                4096,
				IndexSizeChan:       16,
				nBuffersAvaibleData: maxSubIndexPerIndex * 16,
			},
			{
				Size:                16384,
				IndexSizeChan:       4,
				nBuffersAvaibleData: maxSubIndexPerIndex * 4,
			},
			{
				Size:                32768,
				IndexSizeChan:       2,
				nBuffersAvaibleData: maxSubIndexPerIndex * 2,
			},
			{
				Size:                65536,
				IndexSizeChan:       1,
				nBuffersAvaibleData: maxSubIndexPerIndex,
			},
		},
		NWorkers:  8,
		QueueSize: 1024,
	}

	// Creamos la instancia pasando las opciones
	motor := newDacV3(config)

	motor.initDacV3()
}
