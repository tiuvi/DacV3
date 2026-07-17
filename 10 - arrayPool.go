package dacV3

import (
	"sync"
)

// PagedPool es el gestor principal genérico con tamaño dinámico y reuso de IDs
type PagedPool[T any] struct {
	globalLock sync.RWMutex
	chunks     [][]T    // Slice de slices para permitir chunkSize dinámico
	chunkSize  uint32   // Tamaño de cada bloque definido en tiempo de ejecución
	freeIDs    []uint32 // IDs de elementos liberados listos para reuso
	nextID     uint32   // ID secuencial protegido por globalLock
}

// NewPoolArray Constructor genérico con tamaño de bloque dinámico
func NewPoolArray[T any](chunkSize uint32) *PagedPool[T] {
	// Evitamos tamaños de bloque inválidos
	if chunkSize == 0 {
		chunkSize = 4096
	}

	pool := &PagedPool[T]{
		chunks:    make([][]T, 0, 1024),
		chunkSize: chunkSize,
		freeIDs:   make([]uint32, 0, 1024),
		nextID:    0,
	}

	// Inicializamos el primer bloque de memoria dinámicamente.
	// El ID 0 (posición items[0]) no será entregado para mantener la compatibilidad con el ID 0 inválido.
	pool.chunks = append(pool.chunks, make([]T, chunkSize))

	return pool
}

// New obtiene un espacio disponible en el pool (nuevo o reutilizado)
func (p *PagedPool[T]) New() (position uint32, itemPoint *T) {
	p.globalLock.Lock()
	defer p.globalLock.Unlock()

	var id uint32

	// 1. Intentamos obtener un ID previamente liberado
	if len(p.freeIDs) > 0 {
		lastIdx := len(p.freeIDs) - 1
		id = p.freeIDs[lastIdx]
		p.freeIDs = p.freeIDs[:lastIdx]
	} else {
		// Si no hay reutilizables, incrementamos secuencialmente
		p.nextID++
		id = p.nextID
	}

	chunkIdx := id / p.chunkSize
	itemIdx := id % p.chunkSize

	// 2. Expandimos el pool añadiendo nuevos bloques si es necesario
	for int(chunkIdx) >= len(p.chunks) {
		p.chunks = append(p.chunks, make([]T, p.chunkSize))
	}

	return id, &p.chunks[chunkIdx][itemIdx]
}

// Get devuelve el puntero al elemento de forma concurrente y segura
func (p *PagedPool[T]) Get(id uint32) *T {
	if id == 0 {
		return nil
	}

	chunkIdx := int(id / p.chunkSize)
	itemIdx := int(id % p.chunkSize)

	p.globalLock.RLock()
	defer p.globalLock.RUnlock()

	// Validamos límites para prevenir fallos de índice fuera de rango
	if chunkIdx >= len(p.chunks) {
		return nil
	}

	return &p.chunks[chunkIdx][itemIdx]
}

// Delete libera el elemento, limpia su memoria y permite su posterior reuso
func (p *PagedPool[T]) Delete(id uint32) {
	if id == 0 {
		return
	}

	chunkIdx := int(id / p.chunkSize)
	itemIdx := int(id % p.chunkSize)

	p.globalLock.Lock()
	defer p.globalLock.Unlock()

	// Validamos límites antes de manipular el elemento
	if chunkIdx >= len(p.chunks) {
		return
	}

	// Asignamos el valor por defecto de T para evitar fugas de memoria (memory leaks)
	// si T contiene punteros, slices, strings o mapas.
	var zero T
	p.chunks[chunkIdx][itemIdx] = zero

	// Agregamos el ID de vuelta al pool de libres
	p.freeIDs = append(p.freeIDs, id)
}