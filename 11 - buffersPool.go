package dacV3

import (
	"sync"
)

type bufferArena struct {
	mu             sync.RWMutex
	arenas         [][]byte
	freeIDs        []uint32 // Aquí guardaremos los IDs internos (0, 1, 2...)
	nextID         uint32   // Este es el ID interno. Empezará en 0.
	blocksPerArena uint32
	size           int64
	isFree         []bool
}

func newBufferArena(blocksPerArena uint32, size int64) *bufferArena {

	totalSize := int(int64(blocksPerArena) * size)
	
	firstArena := MakeAlignedBlock(totalSize)

	return &bufferArena{
		arenas:         [][]byte{firstArena},
		blocksPerArena: blocksPerArena,
		freeIDs:        make([]uint32, 0, blocksPerArena),
		nextID:         0, // INTERNAMENTE empezamos en 0
		size:           size,
		isFree:         make([]bool, blocksPerArena),
	}
}

func (ib *bufferArena) expandArena() {

	totalSize := int(int64(ib.blocksPerArena) * ib.size)
	newArena := MakeAlignedBlock(totalSize)

	ib.arenas = append(ib.arenas, newArena)

	newIsFree := make([]bool, len(ib.isFree)+int(ib.blocksPerArena))
	copy(newIsFree, ib.isFree)
	ib.isFree = newIsFree
}

func (ib *bufferArena) addBufferArena() (uint32, []byte) {

	ib.mu.Lock()
	defer ib.mu.Unlock()

	var internalID uint32

	if len(ib.freeIDs) > 0 {
		lastIdx := len(ib.freeIDs) - 1
		internalID = ib.freeIDs[lastIdx]
		ib.freeIDs = ib.freeIDs[:lastIdx]
	} else {
		if ib.nextID >= uint32(len(ib.arenas))*ib.blocksPerArena {
			ib.expandArena()
		}
		internalID = ib.nextID
		ib.nextID++
	}

	ib.isFree[internalID] = false

	// Matemáticas usando el ID interno (0, 1, 2...)
	arenaIdx := internalID / ib.blocksPerArena
	blockIdx := internalID % ib.blocksPerArena
	start := int64(blockIdx) * ib.size

	// TRUCO AQUI: Devolvemos internalID + 1 al usuario.
	// Si internalID es 0, el usuario recibe 1.
	return internalID + 1, ib.arenas[arenaIdx][start : start+ib.size]
}

func (ib *bufferArena) getBufferArena(id uint32) []byte {
	// Protegemos si el usuario nos pide el 0 (que es inválido para el usuario)
	if id == 0 {
		return nil
	}

	// Convertimos el ID del usuario al ID interno restando 1
	internalID := id - 1

	ib.mu.RLock()
	defer ib.mu.RUnlock()

	if internalID >= ib.nextID || ib.isFree[internalID] {
		return nil
	}

	arenaIdx := internalID / ib.blocksPerArena
	blockIdx := internalID % ib.blocksPerArena
	start := int64(blockIdx) * ib.size

	return ib.arenas[arenaIdx][start : start+ib.size]
}

func (ib *bufferArena) delBufferArena(id uint32) {
	if id == 0 {
		return
	}

	// Convertimos el ID del usuario al ID interno restando 1
	internalID := id - 1

	ib.mu.Lock()
	defer ib.mu.Unlock()

	if internalID >= ib.nextID || ib.isFree[internalID] {
		return
	}

	arenaIdx := internalID / ib.blocksPerArena
	blockIdx := internalID % ib.blocksPerArena
	start := int64(blockIdx) * ib.size

	clear(ib.arenas[arenaIdx][start : start+ib.size])

	// Guardamos el ID INTERNO en la lista de liberados, no el del usuario
	ib.freeIDs = append(ib.freeIDs, internalID)
	ib.isFree[internalID] = true
}
