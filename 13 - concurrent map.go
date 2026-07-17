package dacV3

import (
	"sync"
)

// MapShard ahora tiene la clave fija a [32]byte, y el valor genérico V
type MapShard[V any] struct {
	sync.RWMutex
	data map[[32]byte]V
}

// ConcurrentMap con valor genérico
type ConcurrentMap[V any] struct {
	ShardMask uint16
	shards    []*MapShard[V]
}

// NewConcurrentMap recibe el número total de shards (potencia de 2)
func NewConcurrentMap[V any](numShards int) *ConcurrentMap[V] {
	m := &ConcurrentMap[V]{
		ShardMask: uint16(numShards - 1),
		shards:    make([]*MapShard[V], numShards),
	}

	for i := 0; i < numShards; i++ {
		m.shards[i] = &MapShard[V]{
			// Tamaño inicial estimado por shard
			data: make(map[[32]byte]V, 100),
		}
	}
	return m
}

// getShardIndex es súper rápido de nuevo porque la clave siempre es [32]byte
func (m *ConcurrentMap[V]) getShardIndex(key [32]byte) uint16 {
	combined := (uint16(key[0]) << 8) | uint16(key[1])
	return combined & m.ShardMask
}

func (m *ConcurrentMap[V]) Get(key [32]byte) (V, bool) {
	shardIndex := m.getShardIndex(key)
	shard := m.shards[shardIndex]

	// Lectura rápida
	shard.RLock()
	defer shard.RUnlock()

	val, exists := shard.data[key]
	return val, exists
}

func (m *ConcurrentMap[V]) StoreOrGet(key [32]byte, allocator func() V) V {

	shardIndex := m.getShardIndex(key)

	shard := m.shards[shardIndex]

	// Lectura rápida
	shard.RLock()
	val, exists := shard.data[key]
	shard.RUnlock()
	if exists {
		return val
	}

	// Escritura segura
	shard.Lock()
	defer shard.Unlock()

	// Doble comprobación
	if val, exists = shard.data[key]; exists {
		return val
	}

	// Asignación con el tipo genérico V
	newVal := allocator()
	shard.data[key] = newVal

	return newVal
}
