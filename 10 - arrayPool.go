package dacV3

import (
	"sync"
	"sync/atomic"
)

const chunkSize = 4096

// Chunk es el bloque de memoria contigua
type Chunk[T any] struct {
	items [chunkSize]T
}

// PagedPool es el gestor principal genérico
type PagedPool[T any] struct {
	globalLock sync.RWMutex
	chunks     []*Chunk[T]
	nextID     atomic.Uint32
}

// NewPoolArray Constructor genérico
func NewPoolArray[T any]() *PagedPool[T] {
	pool := &PagedPool[T]{
		chunks: make([]*Chunk[T], 0, 1024),
	}

	// Añadimos el primer chunk (el ID 0 quedará en la posición items[0] que nunca será devuelta)
	pool.chunks = append(pool.chunks, &Chunk[T]{})

	// CONSEJO 1: Inicializamos en 0.
	// Así, el primer Add(1) devolverá ID = 1. (El ID 0 es ignorado naturalmente).
	pool.nextID.Store(0)

	return pool
}

// New obtiene un nuevo espacio en el pool
func (p *PagedPool[T]) New() (position uint32, itemPoint *T) {
	// 1. Obtenemos el ID atómicamente. Si empezó en 0, el primero será 1.
	id := p.nextID.Add(1)

	chunkIdx := id / chunkSize
	itemIdx := id % chunkSize

	// 2. Verificamos si necesitamos expandir leyendo con RLock
	p.globalLock.RLock()
	needsExpansion := int(chunkIdx) >= len(p.chunks)
	p.globalLock.RUnlock()

	// 3. Expansión (Double-checked locking)
	if needsExpansion {
		p.globalLock.Lock()
		// Verificamos de nuevo por si otra gorutina ya hizo el append
		if int(chunkIdx) >= len(p.chunks) {
			p.chunks = append(p.chunks, &Chunk[T]{})
		}
		p.globalLock.Unlock()
	}

	// 4. Obtenemos el chunk de forma segura
	p.globalLock.RLock()
	chunk := p.chunks[chunkIdx]
	p.globalLock.RUnlock()

	return id, &chunk.items[itemIdx]
}

// Get devuelve el puntero al elemento genérico
func (p *PagedPool[T]) Get(id uint32) *T {
	// CONSEJO 2: Protegemos contra la petición del ID 0 (reservado/inválido) o números negativos.
	if id <= 0 {
		return nil
	}

	chunkIdx := int(id / chunkSize)
	itemIdx := int(id % chunkSize)

	// Protegemos para leer de forma segura el slice 'chunks'
	p.globalLock.RLock()
	defer p.globalLock.RUnlock()

	// CONSEJO 3: Validar que el chunk existe para evitar Panic por "index out of range"
	if chunkIdx >= len(p.chunks) {
		return nil
	}

	return &p.chunks[chunkIdx].items[itemIdx]
}
