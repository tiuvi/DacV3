package main

import (
	"sync"
)


type bufferArena struct {
	mu             sync.RWMutex
	arenas         [][]byte // NUEVO: Lista de bloques gigantes. Jamás se invalidan.
	freeIDs        []int64
	nextID         int64
	blocksPerArena int64 // Cuántos bloques caben en un Arena (ej. 1000)
	size           int64 // Tamaño de cada bloque (ej. 4096)
	isFree         []bool // MEJORA: Array plano. Más rápido y barato que un mapa.
}

func newBufferArena(blocksPerArena int64, size int64) *bufferArena {

	totalSize := int(blocksPerArena * size)

	// Creamos el primer bloque gigante
	firstArena := MakeAlignedBlock(totalSize)

	return &bufferArena{
		arenas:         [][]byte{firstArena},
		blocksPerArena: blocksPerArena,
		freeIDs:        make([]int64, 0, blocksPerArena),
		nextID:         0,
		size:           size,
		isFree:         make([]bool, blocksPerArena),
	}
}

func (ib *bufferArena) expandArena() {


	totalSize := int(ib.blocksPerArena * ib.size)

	newArena := MakeAlignedBlock(totalSize)

	ib.arenas = append(ib.arenas, newArena)

	// 2. Expandimos el tracker de libres para que cubra el nuevo Arena
	newIsFree := make([]bool, len(ib.isFree)+int(ib.blocksPerArena))

	copy(newIsFree, ib.isFree) // Copiar []bool es ultrarrápido

	ib.isFree = newIsFree
}

func (ib *bufferArena) addBufferArena() (int64, []byte) {

	ib.mu.Lock()
	defer ib.mu.Unlock()

	var id int64

	if len(ib.freeIDs) > 0 {

		lastIdx := len(ib.freeIDs) - 1

		id = ib.freeIDs[lastIdx]

		ib.freeIDs = ib.freeIDs[:lastIdx]

	} else {

		// Evaluamos si necesitamos un nuevo bloque gigante
		if ib.nextID >= int64(len(ib.arenas))*ib.blocksPerArena {
			ib.expandArena()
		}

		id = ib.nextID

		ib.nextID++
	}

	// Marcamos explícitamente que ya no está libre
	ib.isFree[id] = false

	// DEVOLVEMOS EL SLICE USANDO MATEMÁTICAS O(1)
	arenaIdx := id / ib.blocksPerArena

	blockIdx := id % ib.blocksPerArena

	start := blockIdx * ib.size

	return id, ib.arenas[arenaIdx][start : start+ib.size]
}

func (ib *bufferArena) getBufferArena(id int64) []byte {

	ib.mu.RLock()
	defer ib.mu.RUnlock()

	if id >= ib.nextID || ib.isFree[id] {
		return nil
	}

	// Buscamos en qué Arena está y en qué posición
	arenaIdx := id / ib.blocksPerArena

	blockIdx := id % ib.blocksPerArena

	start := blockIdx * ib.size

	// Este slice es 100% seguro para siempre.
	return ib.arenas[arenaIdx][start : start+ib.size]
}

func (ib *bufferArena) delBufferArena(id int64) {

	ib.mu.Lock()
	defer ib.mu.Unlock()

	if id >= ib.nextID || ib.isFree[id] {
		return
	}

	arenaIdx := id / ib.blocksPerArena

	blockIdx := id % ib.blocksPerArena

	start := blockIdx * ib.size

	// Limpiamos la memoria
	clear(ib.arenas[arenaIdx][start : start+ib.size])

	ib.freeIDs = append(ib.freeIDs, id)
	
	ib.isFree[id] = true // Lo marcamos como libre
}
