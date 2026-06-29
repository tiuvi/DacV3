package main

import (
	"errors"
	"sync"
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

ESCRITURAS

Primer caso la escritura mas el archivo es superior al tamaño total del archivo
necesitamos una nueva pagina

	-Primero se escriben los datos en una pagina nueva
	-Segundo se escribe en el indice nuevo
	-tercero se borra el indice viejo en el anterior bloque de indices

	Recuperacion:
		En caso de que existan ambos indices el que tenga una secuencia superior gana
		En caso de que el nuevo indice este corrompido, prevalece el indice viejo con los datos antiguos

Segundo caso escritura en un offset superior al tamaño del archivo pero sin ser mas grande que el archivo total

	-Primero se escribe un wal donde consta donde es la escritura offset y un checksum con cr32
	-Segundo se escriben los datos directos en el archivo
	-Tercero se borra el wall

	Recuperacion:
		-Si existe el wall se compara los datos que se han editado con el checksum en caso de que no coincida
		los datos se borran.

	ADICIONAL:
		Esta escritura permite archivos de grandes puedan ser escritos directamente los datos sin ser reubicados


Tercer caso escritura en un offset inferior al tamaño

	-primero se escribe en un wall los datos completos y donde van
	-segundo se escribe en la pagina donde corresponda.
	-tercero se borra el wall
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

// Datos
type Data struct {
	mu sync.Mutex // Añadimos el mutex para proteger el acceso concurrente
	//Tamaño de la pagina
	sizePage uint64
	//Tamaño del archivo
	sizeFile uint64
	//Indice del array sincronizado
	idBuffer uint64
}

// Array que almacenara los datos de todos los archivos
var pageBytes = make([]Data, 0)

// Mapa de nombres con direccion al array con los datos de cada archivo
var files map[[32]byte]int


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

type SizeConfig struct {
	Size int64
	IndexSizeChan int64
	DataSize  int64
}

// Configuración de tamaños soportados
var supportedConfigs = []SizeConfig{
	{Size:4096, IndexSizeChan: 100, DataSize: 1024},
	{Size:16384,IndexSizeChan: 100, DataSize: 1024 / 2},
	{Size:65536,IndexSizeChan: 100, DataSize: 1024 / 4},
}

func main() {

	//Primero iniciamos dac
	dacV3File = newDacV3(65, 16384, 5, 1000 ,supportedConfigs , 20 , 8192)

	dacV3File.initDacV3()


	
}
