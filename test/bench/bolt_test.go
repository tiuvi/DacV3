package test

import (
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"sync/atomic"
	"testing"

	"dacV3" // Asegúrate de que coincida con tu go.mod

	"go.etcd.io/bbolt"
)

// Nombre del bucket que usará BoltDB
var boltBucketName = []byte("BenchmarkBucket")

// ==========================================
// 1. BENCHMARK DE ESCRITURA CONCURRENTE
// ==========================================
func BenchmarkBolt_WriteConcurrent(b *testing.B) {

	dir := filepath.Join(os.Getenv("dir") , "bolt.db")

	interactionEnv := os.Getenv("interaction")
	interaction, err := strconv.ParseInt(interactionEnv, 10, 64)
	if err != nil {
		b.Fatal(err)
	}

	// Abrimos Bolt (Por defecto Bolt hace fsync tras cada Update,
	// por lo que es equivalente a SyncWrites=true de Badger)
	db, err := bbolt.Open(dir, 0600, nil)
	if err != nil {
		b.Fatal(err)
	}
	defer db.Close()

	// ---------------------------------------------------------
	// FASE DE PREPARACIÓN (NO SE MIDE)
	// Creamos el Bucket obligatiorio de Bolt y el pool de datos
	// ---------------------------------------------------------
	err = db.Update(func(tx *bbolt.Tx) error {
		_, err := tx.CreateBucketIfNotExists(boltBucketName)
		return err
	})
	if err != nil {
		b.Fatal(err)
	}

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

	// === AQUÍ SE REINICIA EL RELOJ ===
	b.ResetTimer()

	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			idx := atomic.AddInt64(&counter, 1) % interaction

			// AQUÍ ADENTRO SOLO ESTÁ LA INSERCIÓN PURA
			err := db.Update(func(tx *bbolt.Tx) error {
				bucket := tx.Bucket(boltBucketName)
				return bucket.Put(keys[idx], vals[idx])
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
func BenchmarkBolt_ReadConcurrent(b *testing.B) {

	dir := filepath.Join(os.Getenv("dir") , "bolt.db")

	interactionEnv := os.Getenv("interaction")
	interaction, err := strconv.ParseInt(interactionEnv, 10, 64)
	if err != nil {
		b.Fatal(err)
	}

	db, err := bbolt.Open(dir, 0600, nil)
	if err != nil {
		b.Fatal(err)
	}
	defer db.Close()

	// ---------------------------------------------------------
	// FASE DE PREPARACIÓN (NO SE MIDE)
	// Creamos el Bucket, pre-generamos llaves e insertamos
	// ---------------------------------------------------------
	err = db.Update(func(tx *bbolt.Tx) error {
		_, err := tx.CreateBucketIfNotExists(boltBucketName)
		return err
	})
	if err != nil {
		b.Fatal(err)
	}

	keys := make([][]byte, interaction)

	for i := 0; i < int(interaction); i++ {
		keyName := fmt.Sprintf("pagina_%d", i)
		key32 := dacV3.NewUUIDSheedBytes([]byte(keyName))

		k := make([]byte, 32)
		copy(k, key32[:])
		keys[i] = k

		val := []byte(fmt.Sprintf("%s %d", textWrite, i))

		// Llenamos la DB (esto no se medirá)
		_ = db.Update(func(tx *bbolt.Tx) error {
			bucket := tx.Bucket(boltBucketName)
			return bucket.Put(k, val)
		})
	}

	var counter int64

	// === AQUÍ SE REINICIA EL RELOJ ===
	b.ResetTimer()

	b.RunParallel(func(pb *testing.PB) {
		localBuffer := make([]byte, 8192) // Evita allocations en lectura

		for pb.Next() {
			idx := atomic.AddInt64(&counter, 1) % interaction

			// AQUÍ ADENTRO SOLO ESTÁ LA LECTURA PURA
			err := db.View(func(tx *bbolt.Tx) error {
				bucket := tx.Bucket(boltBucketName)
				val := bucket.Get(keys[idx])
				if val == nil {
					return fmt.Errorf("llave no encontrada")
				}

				// Copiamos a un buffer local para simular la lectura de página real
				_ = copy(localBuffer, val)
				return nil
			})

			if err != nil {
				b.Error(err)
			}
		}
	})
}
