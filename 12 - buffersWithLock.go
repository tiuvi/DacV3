package dacV3

import (
	"sync"
)

// GlobalBuffer representa la estructura de control que maneja el segmento de buffer,
// sus límites y su estado dentro del pool.
type GlobalBuffer struct {
	mu         sync.RWMutex
	initOffset int64
	endOffset  int64
	active     bool   
	count int64
	
	buf        []byte // Segmento de memoria asignado desde la arena
	bufID      uint32 // ID interno de la memoria asignada en la arena

}

// GlobalBufferPool unifica el pool indexado dinámicamente de estructuras de control
// junto con el gestor de asignación física de memoria (arena) para los buffers.
type GlobalBufferPool struct {
	mu sync.RWMutex

	// --- Pool de estructuras GlobalBuffer (Dinámico) ---
	chunks    [][]GlobalBuffer // Slice de slices para permitir chunkSize dinámico
	chunkSize uint32          // Tamaño de cada chunk definido al inicializar
	freeIDs   []uint32        // IDs de estructuras disponibles para reutilización
	nextID    uint32          // Siguiente ID secuencial para nuevas estructuras

	// --- Arena de memoria para buffers ---
	arenas         [][]byte
	blocksPerArena uint32
	blockSize      int64
	freeBufIDs     []uint32 // IDs de bloques de memoria de la arena listos para reuso
	nextBufID      uint32   // Siguiente ID secuencial de bloque en la arena
	isBufFree      []bool   // Control de estado de liberación física por bloque
}

// NewGlobalBufferPool inicializa el pool combinado con tamaños definidos en tiempo de ejecución.
func NewGlobalBufferPool(chunkSize uint32, blocksPerArena uint32, blockSize int64) *GlobalBufferPool {

	totalSize := int(int64(blocksPerArena) * blockSize)
	
	firstArena := MakeAlignedBlock(totalSize)

	pool := &GlobalBufferPool{
		chunks:         make([][]GlobalBuffer, 0, 1024),
		chunkSize:      chunkSize,
		freeIDs:        make([]uint32, 0, 1024),
		nextID:         0,
		arenas:         [][]byte{firstArena},
		blocksPerArena: blocksPerArena,
		blockSize:      blockSize,
		freeBufIDs:     make([]uint32, 0, blocksPerArena),
		nextBufID:      0,
		isBufFree:      make([]bool, blocksPerArena),
	}

	// Inicializamos el primer chunk dinámicamente con el tamaño configurado
	pool.chunks = append(pool.chunks, make([]GlobalBuffer, chunkSize))

	return pool
}

// expandChunks asegura la disponibilidad de chunks según el chunkSize configurado
func (p *GlobalBufferPool) expandChunks(internalID uint32) {
	chunkIdx := int(internalID / p.chunkSize)
	for chunkIdx >= len(p.chunks) {
		p.chunks = append(p.chunks, make([]GlobalBuffer, p.chunkSize))
	}
}

// expandArena reserva un nuevo bloque físico de memoria alineada
func (p *GlobalBufferPool) expandArena() {
	totalSize := int(int64(p.blocksPerArena) * p.blockSize)
	newArena := MakeAlignedBlock(totalSize)

	p.arenas = append(p.arenas, newArena)

	newIsBufFree := make([]bool, len(p.isBufFree)+int(p.blocksPerArena))
	copy(newIsBufFree, p.isBufFree)
	p.isBufFree = newIsBufFree
}

// New solicita una estructura GlobalBuffer y le asocia un bloque de memoria []byte de la arena.
func (p *GlobalBufferPool) New() (uint32, *GlobalBuffer) {
	p.mu.Lock()
	defer p.mu.Unlock()

	// 1. Obtención del ID para la estructura de control GlobalBuffer
	var structID uint32
	if len(p.freeIDs) > 0 {
		lastIdx := len(p.freeIDs) - 1
		structID = p.freeIDs[lastIdx]
		p.freeIDs = p.freeIDs[:lastIdx]
	} else {
		structID = p.nextID
		p.nextID++
		p.expandChunks(structID)
	}

	// 2. Obtención de un bloque de memoria física de la arena
	var bufID uint32
	if len(p.freeBufIDs) > 0 {
		lastIdx := len(p.freeBufIDs) - 1
		bufID = p.freeBufIDs[lastIdx]
		p.freeBufIDs = p.freeBufIDs[:lastIdx]
	} else {
		if p.nextBufID >= uint32(len(p.arenas))*p.blocksPerArena {
			p.expandArena()
		}
		bufID = p.nextBufID
		p.nextBufID++
	}

	p.isBufFree[bufID] = false

	// Calcular límites del segmento físico de la arena correspondiente
	arenaIdx := bufID / p.blocksPerArena
	blockIdx := bufID % p.blocksPerArena
	start := int64(blockIdx) * p.blockSize

	// 3. Inicializar la estructura GlobalBuffer en el pool indexado usando chunkSize dinámico
	chunkIdx := structID / p.chunkSize
	itemIdx := structID % p.chunkSize
	pBuf := &p.chunks[chunkIdx][itemIdx]

	pBuf.mu.Lock()
	pBuf.initOffset = 0
	pBuf.endOffset = 0
	pBuf.buf = p.arenas[arenaIdx][start : start+p.blockSize]
	pBuf.bufID = bufID
	pBuf.active = true
	pBuf.mu.Unlock()

	return structID + 1, pBuf
}

// Get devuelve el puntero al GlobalBuffer según el ID de usuario provisto.
func (p *GlobalBufferPool) Get(id uint32) *GlobalBuffer {
	if id == 0 {
		return nil
	}

	structID := id - 1

	p.mu.RLock()
	defer p.mu.RUnlock()

	if structID >= p.nextID {
		return nil
	}

	chunkIdx := structID / p.chunkSize
	itemIdx := structID % p.chunkSize
	pBuf := &p.chunks[chunkIdx][itemIdx]

	if !pBuf.active {
		return nil
	}

	return pBuf
}

// Free libera el buffer y la estructura del pool, dejándolos disponibles para reuso.
func (p *GlobalBufferPool) Free(id uint32) {
	if id == 0 {
		return
	}

	structID := id - 1

	p.mu.Lock()
	defer p.mu.Unlock()

	if structID >= p.nextID {
		return
	}

	chunkIdx := structID / p.chunkSize
	itemIdx := structID % p.chunkSize
	pBuf := &p.chunks[chunkIdx][itemIdx]

	if !pBuf.active {
		return
	}

	pBuf.mu.Lock()
	clear(pBuf.buf)

	p.freeBufIDs = append(p.freeBufIDs, pBuf.bufID)
	p.isBufFree[pBuf.bufID] = true

	pBuf.buf = nil
	pBuf.bufID = 0
	pBuf.initOffset = 0
	pBuf.endOffset = 0
	pBuf.active = false
	pBuf.mu.Unlock()

	p.freeIDs = append(p.freeIDs, structID)
}