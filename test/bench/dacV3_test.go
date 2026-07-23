package test

import (
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"sync/atomic"
	"testing"

	"dacV3" // Importamos tu base de datos
)

// ==========================================
// 1. BENCHMARK DE ESCRITURA CONCURRENTE
// ==========================================
func BenchmarkDacV3_WriteConcurrent(b *testing.B) {

	dir := filepath.Join(os.Getenv("dir"), "dacV3.db")

	interactionEnv := os.Getenv("interaction")
	interaction, err := strconv.ParseInt(interactionEnv, 10, 64)
	if err != nil {
		b.Fatal(err)
	}

	// Inicializamos tu BD con la configuración
	config := dacV3.NewDacV3Options(dir, true, 1)

	db := dacV3.InitDacV3(config)

	// IMPORTANTE: Asegúrate de tener una función para cerrar/limpiar la DB al terminar
	// defer db.Close()

	// ---------------------------------------------------------
	// FASE DE PREPARACIÓN (NO SE MIDE)
	// Creamos el "pool" de datos. Notarás que aquí guardamos
	// directamente el [32]byte que usa tu función WritePage.
	// ---------------------------------------------------------

	keys := make([][32]byte, interaction) // Tu DB usa un array fijo de 32 bytes
	vals := make([][]byte, interaction)

	for i := 0; i < int(interaction); i++ {
		keyName := fmt.Sprintf("pagina_%d", i)

		// Guardamos la llave nativa [32]byte directamente
		keys[i] = dacV3.NewUUIDSheedBytes([]byte(keyName))

		vals[i] = []byte(fmt.Sprintf("%s %d", textWrite, i))
	}

	var counter int64

	// === AQUÍ SE REINICIA EL RELOJ ===
	b.ResetTimer()

	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			// Obtenemos el índice de nuestro pool
			idx := atomic.AddInt64(&counter, 1) % interaction

			// AQUÍ ADENTRO SOLO ESTÁ LA INSERCIÓN PURA DE TU DB
			// WritePage(key [32]byte, buffer []byte, flag int)
			err := db.WritePage(keys[idx], vals[idx], 0)

			if err != nil {
				b.Error(err)
			}
		}
	})
}

func BenchmarkDacV3_WriteConcurrentWal(b *testing.B) {

	dir := filepath.Join(os.Getenv("dir"), "dacV3.db")

	interactionEnv := os.Getenv("interaction")
	interaction, err := strconv.ParseInt(interactionEnv, 10, 64)
	if err != nil {
		b.Fatal(err)
	}

	// Inicializamos tu BD con la configuración
	config := dacV3.NewDacV3Options(dir, true, 1)

	db := dacV3.InitDacV3(config)

	keys := make([][32]byte, interaction) // Tu DB usa un array fijo de 32 bytes
	vals := make([][]byte, interaction)

	for i := 0; i < int(interaction); i++ {

		keyName := fmt.Sprintf("pagina_%d", i)

		// Guardamos la llave nativa [32]byte directamente
		keys[i] = dacV3.NewUUIDSheedBytes([]byte(keyName))

		vals[i] = []byte(fmt.Sprintf("%s %d", textWrite, i))

		val := []byte(fmt.Sprintf("%s %d", textWrite, i))

		_ = db.WritePage(keys[i], val, 0)
	}


	var counter int64

	// === AQUÍ SE REINICIA EL RELOJ ===
	b.ResetTimer()

	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			// Obtenemos el índice de nuestro pool
			idx := atomic.AddInt64(&counter, 1) % interaction

			// AQUÍ ADENTRO SOLO ESTÁ LA INSERCIÓN PURA DE TU DB
			// WritePage(key [32]byte, buffer []byte, flag int)
			err := db.WritePage(keys[idx], vals[idx], 0)

			if err != nil {
				b.Error(err)
			}
		}
	})
}

// ==========================================
// 2. BENCHMARK DE LECTURA CONCURRENTE
// ==========================================
func BenchmarkDacV3_ReadConcurrent(b *testing.B) {

	dir := filepath.Join(os.Getenv("dir"), "dacV3.db")

	interactionEnv := os.Getenv("interaction")
	interaction, err := strconv.ParseInt(interactionEnv, 10, 64)
	if err != nil {
		b.Fatal(err)
	}

	config := dacV3.NewDacV3Options(dir, false, 1)
	db := dacV3.InitDacV3(config)
	// defer db.Close()

	// ---------------------------------------------------------
	// FASE DE PREPARACIÓN (NO SE MIDE)
	// 1. Pre-generamos las llaves
	// 2. Insertamos la data para que ReadPage tenga qué leer
	// ---------------------------------------------------------

	keys := make([][32]byte, interaction)

	for i := 0; i < int(interaction); i++ {
		keyName := fmt.Sprintf("pagina_%d", i)
		keys[i] = dacV3.NewUUIDSheedBytes([]byte(keyName))
		val := []byte(fmt.Sprintf("%s %d", textWrite, i))

		// Llenamos la DB (esto no se medirá)
		_ = db.WritePage(keys[i], val, 0)
	}

	var counter int64

	// === AQUÍ SE REINICIA EL RELOJ ===
	b.ResetTimer()

	b.RunParallel(func(pb *testing.PB) {
		// En tu código original usabas un buffer de 128 bytes.
		// Recomiendo usar uno que cubra el tamaño de "textWrite" (ej. 8192)
		// para evitar que tu DB devuelva error por buffer pequeño, si es que lo valida.
		localReadBuffer := make([]byte, 8192)

		for pb.Next() {
			idx := atomic.AddInt64(&counter, 1) % interaction

			// AQUÍ ADENTRO SOLO ESTÁ LA LECTURA PURA DE TU DB
			_, err := db.ReadPage(keys[idx], localReadBuffer, 0)

			if err != nil {
				b.Error(err)
			}

			println(idx, string(localReadBuffer))
		}
	})
}
