package main

import "sync"

// Datos
type Page struct {

	mu sync.Mutex // Añadimos el mutex para proteger el acceso concurrente
	//Indice del buffer sincronizado
	idBuffer uint32
	//Indice del inice al que pertenece la pagina
	idIndex uint32
	//Indice del subindice al que pertenece la pagina
	idSubIndex uint8 
}


// Mapa de nombres con direccion al array con los datos de cada archivo
var files map[[32]byte]int

// Array que almacenara los datos de todos los archivos
var pageBytes = make([]Page, 0)


func (sfDacV3 *dacV3) InitPage(index Index ,offset int64) (page *Page, err error) {

	return
}

func (sfDacV3 *dacV3) ReadPage(hash [32]byte) (page *Page, err error) {

/*
	files[hash]

	arena, found := sfDacV3.dataPools[len(data)]
	if !found {
		return errArenaNotFound
	}

	id, buffer := arena.addBufferArena()


	 Page  {
    mu sync.Mutex // Añadimos el mutex para proteger el acceso concurrente
    //Tamaño de la pagina
    sizePage uint64
    //Tamaño del archivo
    sizeFile uint64
    //Indice del array sincronizado
    idBuffer uint64
}
*/
	return
}
