package main

import (
	"errors"
	"sync/atomic"
)

/*
/media/franky/tiuviweb/go/bin/go mod tidy

/media/franky/tiuviweb/go/bin/go run main.go


/media/franky/tiuviweb/go/bin/go mod init dacv3
/media/franky/tiuviweb/go/bin/go get golang.org/x/sys/unix
/media/franky/tiuviweb/go/bin/go run main.go

APERTURAS

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

Primer caso la escritura mas el archivo es superior al tamaño total del archivo
necesitamos una nueva pagina

	-Primero se escriben los datos en una pagina nueva
		funcion -> WritePageDirect

	-Segundo se escribe en el indice nuevo
		funcion -> WriteIndexDirect

	-tercero se borra el indice viejo en el anterior bloque de indices
		funcion -> WriteIndexDirect

	Recuperacion:
		En caso de que existan ambos indices el que tenga una secuencia superior gana
		En caso de que el nuevo indice este corrompido, prevalece el indice viejo con los datos antiguos



Segundo caso escritura en un offset superior al tamaño del archivo pero sin ser mas grande que el archivo total

	-Primero se escribe un wal donde consta donde es la escritura offset y un checksum con cr32
	-Segundo se escriben los datos directos en el archivo
		funcion -> WritePageDirect


	Recuperacion:
		-Si existe el wall se compara los datos que se han editado con el checksum en caso de que no coincida
		los datos se borran.

	ADICIONAL:
		Esta escritura permite archivos de grandes puedan ser escritos directamente los datos sin ser reubicados


Tercer caso escritura en un offset inferior al tamaño

	-primero se escribe en un wall los datos completos y donde van
	-segundo se escribe en la pagina donde corresponda.
		funcion -> WritePageWall

	Recuperacion:
		-Si existen los wall se encolan en orden


LECTURAS

Primer caso si la pagina no ha sido abierta

	-Primero se busca el hash
	-Se pide un bufer
	-Se sincroniza los datos con el disco

Segundo caso si la pagina ya ha sido abierta

	-Se responde directamente desde el buffer

Tercer caso lecturas de archivos grandes

	-Se responde con una lectura por rangos directamente desde el disco


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

// Configuración de tamaños soportados
var supportedConfigs = []SizeConfig{
	{Size: 4096, IndexSizeChan: 100, nBuffersAvaibleData: 1024},
	{Size: 16384, IndexSizeChan: 100, nBuffersAvaibleData: 1024 / 2},
	{Size: 65536, IndexSizeChan: 100, nBuffersAvaibleData: 1024 / 4},
}

type DacV3Options struct {
	MaxReserveSize        int64
	SsdNIopsMili          uint32
	SsdPageSize           uint32
	TotalWallBuffer       uint32
	NBuffersAvailableIndex uint32 // Corregido 'Avaible' a 'Available' para mayor precisión
	SupportedSizes        []SizeConfig
	NWorkers              int
	QueueSize             int
}

func main() {

	// Definimos las opciones de forma muy visual y explícita
	config := DacV3Options{
		MaxReserveSize:         1024 * 1024 * 100, // 100 MB
		SsdNIopsMili:           50000,
		SsdPageSize:            4096,
		TotalWallBuffer:        256,
		NBuffersAvailableIndex: 128,
		SupportedSizes: []SizeConfig{
			{Size: 4096},
			{Size: 16384},
			{Size: 65536},
		},
		NWorkers:  8,
		QueueSize: 1024,
	}

	// Creamos la instancia pasando las opciones
	motor := newDacV3(config)

	motor.initDacV3()
}

type BitMap []byte

func (b BitMap) Set(bitID int) {
	b[bitID/8] |= 1 << (7 - (bitID % 8))
}

func (b BitMap) Get(bitID int) bool {
	return (b[bitID/8] & (1 << (7 - (bitID % 8)))) != 0
}

func (b BitMap) UnSet(bitID int) {
	b[bitID/8] &^= 1 << (7 - (bitID % 8))
}

func (b BitMap) GetBitsOff(n int) (int, bool) {
	start := -1
	count := 0

	for i := 0; i < len(b)*8; i++ {
		if !b.Get(i) {
			if start == -1 {
				start = i
			}
			count++

			if count == n {
				return start, true
			}
		} else {
			start = -1
			count = 0
		}
	}

	return 0, false
}
