package test

import (
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"sync/atomic"
	"testing"

	"dacV3"

	"github.com/dgraph-io/badger/v4"
)

// ==========================================
// 1. BENCHMARK DE ESCRITURA CONCURRENTE
// ==========================================
func BenchmarkBadger_WriteConcurrent(b *testing.B) {

	dir := filepath.Join(os.Getenv("dir") , "badger")

	interactionEnv := os.Getenv("interaction")
	interaction, err := strconv.ParseInt(interactionEnv, 10, 64)
	if err != nil {
		b.Fatal(err)
	}

	opts := badger.DefaultOptions(dir).
		WithSyncWrites(true).
		WithLoggingLevel(badger.WARNING)

	db, err := badger.Open(opts)
	if err != nil {
		b.Fatal(err)
	}
	defer db.Close()

	// ---------------------------------------------------------
	// FASE DE PREPARACIÓN (NO SE MIDE)
	// Creamos un "pool" de llaves y valores en RAM para no
	// gastar CPU calculándolos durante el Benchmark.
	// ---------------------------------------------------------

	keys := make([][]byte, interaction)
	vals := make([][]byte, interaction)

	for i := 0; i < int(interaction); i++ {
		keyName := fmt.Sprintf("pagina_%d", i)
		key32 := dacV3.NewUUIDSheedBytes([]byte(keyName))

		// Copiamos la llave a un nuevo slice seguro
		k := make([]byte, 32)
		copy(k, key32[:])

		keys[i] = k
		vals[i] = []byte(fmt.Sprintf("%s %d", textWrite, i))
	}

	var counter int64

	// === AQUÍ SE REINICIA EL RELOJ ===
	b.ResetTimer()

	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			// Obtenemos el índice de nuestro pool
			idx := atomic.AddInt64(&counter, 1) % interaction

			// AQUÍ ADENTRO SOLO ESTÁ LA INSERCIÓN PURA
			err := db.Update(func(txn *badger.Txn) error {
				return txn.Set(keys[idx], vals[idx])
			})

			if err != nil {
				b.Error(err)
			}
		}
	})
}

// ==========================================
// 2. BENCHMARK DE LECTURA CONCURRENTE
// ==========================================
func BenchmarkBadger_ReadConcurrent(b *testing.B) {

	dir := filepath.Join(os.Getenv("dir") , "badger")

	interactionEnv := os.Getenv("interaction")
	interaction, err := strconv.ParseInt(interactionEnv, 10, 64)
	if err != nil {
		b.Fatal(err)
	}

	opts := badger.DefaultOptions(dir).
		WithSyncWrites(true).
		WithLoggingLevel(badger.WARNING)

	db, err := badger.Open(opts)
	if err != nil {
		b.Fatal(err)
	}
	defer db.Close()

	// ---------------------------------------------------------
	// FASE DE PREPARACIÓN (NO SE MIDE)
	// 1. Pre-generamos las llaves
	// 2. Insertamos la data en Badger para poder leerla
	// ---------------------------------------------------------

	keys := make([][]byte, interaction)

	for i := 0; i < int(interaction); i++ {
		keyName := fmt.Sprintf("pagina_%d", i)
		key32 := dacV3.NewUUIDSheedBytes([]byte(keyName))

		k := make([]byte, 32)
		copy(k, key32[:])
		keys[i] = k

		val := []byte(fmt.Sprintf("%s %d", textWrite, i))

		// Llenamos la DB (esto no se medirá)
		_ = db.Update(func(txn *badger.Txn) error {
			return txn.Set(k, val)
		})
	}

	var counter int64

	// === AQUÍ SE REINICIA EL RELOJ ===
	b.ResetTimer()

	b.RunParallel(func(pb *testing.PB) {
		localBuffer := make([]byte, 8192) // Buffer pre-reservado

		for pb.Next() {
			idx := atomic.AddInt64(&counter, 1) % interaction

			// AQUÍ ADENTRO SOLO ESTÁ LA LECTURA PURA (las llaves ya están hechas)
			err := db.View(func(txn *badger.Txn) error {
				item, err := txn.Get(keys[idx])
				if err != nil {
					return err
				}
				return item.Value(func(val []byte) error {
					_ = copy(localBuffer, val)
					return nil
				})
			})

			if err != nil {
				b.Error(err)
			}
		}
	})
}
