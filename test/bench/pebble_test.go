package test

import (
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"sync/atomic"
	"testing"

	"dacV3"

	"github.com/cockroachdb/pebble"
)

// ==========================================
// 1. BENCHMARK DE ESCRITURA CONCURRENTE
// ==========================================
func BenchmarkPebble_WriteConcurrent(b *testing.B) {
	// USAMOS LA RUTA DINÁMICA
	dir := filepath.Join(os.Getenv("dir") , "pebble")

	interactionEnv := os.Getenv("interaction")
	interaction , err := strconv.ParseInt(interactionEnv , 10 , 64)
	if err != nil {
		b.Fatal(err)
	}

	db, err := pebble.Open(dir, &pebble.Options{})
	if err != nil {
		b.Fatal(err)
	}
	defer db.Close()

	// ---------------------------------------------------------
	// FASE DE PREPARACIÓN
	// ---------------------------------------------------------

	keys := make([][]byte, interaction)
	vals := make([][]byte, interaction)

	for i := 0; i < int(interaction); i++ {
		keyName := fmt.Sprintf("pagina_%d", i)
		key32 := dacV3.NewUUIDSheedBytes([]byte(keyName))

		k := make([]byte, 32)
		copy(k, key32[:])
		keys[i] = k

		vals[i] = []byte(fmt.Sprintf("%s %d", textWrite, i))
	}

	var counter int64

	// === REINICIO DE RELOJ ===
	b.ResetTimer()

	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			idx := atomic.AddInt64(&counter, 1) % interaction

			// ESCRITURA PURA: Usamos pebble.Sync para ser justos con Badger y Bolt
			err := db.Set(keys[idx], vals[idx], pebble.Sync)
			if err != nil {
				b.Error(err)
			}
		}
	})
}

// ==========================================
// 2. BENCHMARK DE LECTURA CONCURRENTE
// ==========================================
func BenchmarkPebble_ReadConcurrent(b *testing.B) {

	dir := filepath.Join(os.Getenv("dir") , "pebble")
	
	interactionEnv := os.Getenv("interaction")
	interaction , err := strconv.ParseInt(interactionEnv , 10 , 64)
	if err != nil {
		b.Fatal(err)
	}

	db, err := pebble.Open(dir, &pebble.Options{})
	if err != nil {
		b.Fatal(err)
	}
	defer db.Close()

	// ---------------------------------------------------------
	// FASE DE PREPARACIÓN (Llenado previo)
	// ---------------------------------------------------------

	keys := make([][]byte, interaction)

	for i := 0; i < int(interaction); i++ {
		keyName := fmt.Sprintf("pagina_%d", i)
		key32 := dacV3.NewUUIDSheedBytes([]byte(keyName))

		k := make([]byte, 32)
		copy(k, key32[:])
		keys[i] = k

		val := []byte(fmt.Sprintf("%s %d", textWrite, i))

		// Guardamos
		_ = db.Set(k, val, pebble.Sync)
	}

	var counter int64

	// === REINICIO DE RELOJ ===
	b.ResetTimer()

	b.RunParallel(func(pb *testing.PB) {
		localBuffer := make([]byte, 8192)

		for pb.Next() {
			idx := atomic.AddInt64(&counter, 1) % interaction

			// LECTURA PURA
			val, closer, err := db.Get(keys[idx])
			if err != nil {
				b.Error(err)
				continue
			}

			// Copiamos al buffer local
			_ = copy(localBuffer, val)

			// ¡MUY IMPORTANTE cerrar el closer en Pebble para liberar RAM!
			closer.Close()
		}
	})
}
