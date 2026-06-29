package main

import (
	"sync"
	"sync/atomic"
)

const chunkSize = 4096

// 1. El bloque de memoria contigua, ahora genérico [T any]
// Eliminamos el sync.RWMutex interno porque asumimos que la sincronización
// de mutaciones en los items recae en el propio struct T o se accede de forma segura.
type Chunk[T any] struct {
	items [chunkSize]T
}

// 2. El gestor principal genérico
type PagedPool[T any] struct {
	globalLock sync.RWMutex
	chunks     []*Chunk[T]
	nextID     atomic.Int64
}

// Constructor genérico
func NewPagedPool[T any]() *PagedPool[T] {
	pool := &PagedPool[T]{
		chunks: make([]*Chunk[T], 0, 1024),
	}

	// Añadimos el primer chunk (el ID 0 queda reservado/vacío)
	pool.chunks = append(pool.chunks, &Chunk[T]{})
	pool.nextID.Store(1)

	return pool
}

func (p *PagedPool[T]) New() (position int64, itemPoint *T) {
	// 1. Obtenemos el ID atómicamente
	id := p.nextID.Add(1)

	chunkIdx := id / chunkSize
	itemIdx := id % chunkSize

	// 2. Verificamos expansión con RLock
	p.globalLock.RLock()
	needsExpansion := int(chunkIdx) >= len(p.chunks)
	p.globalLock.RUnlock()

	// 3. Expansión (Double-checked locking)
	if needsExpansion {
		p.globalLock.Lock()
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
func (p *PagedPool[T]) Get(id int64) *T {

	chunkIdx := id / chunkSize

	itemIdx := id % chunkSize

	// Solo bloqueamos para leer de forma segura el slice 'chunks'
	p.globalLock.RLock()
	defer p.globalLock.RUnlock()

	// El usuario es responsable de manejar los locks internos de T
	// si va a mutarlo concurrentemente después de obtenerlo.
	return &p.chunks[chunkIdx].items[itemIdx]
}
