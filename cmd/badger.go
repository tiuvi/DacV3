package main

import (
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	. "dacV3" // Tu librería
	"github.com/dgraph-io/badger/v4"
)

/* 
  IMPORTANTE: Cuando abras la base de datos de Badger en tu función main, 
  DEBES hacerlo con SyncWrites = true para que sea una comparativa real.
  Ejemplo:
  
  opts := badger.DefaultOptions("/ruta/a/badger").WithSyncWrites(true)
  dbBadger, err := badger.Open(opts)
*/

func runTestBadger(db *badger.DB) {
	// ==========================================
	// PREPARACIÓN DE DATOS Y WORKERS (NO se mide)
	// ==========================================

	keys := make([][32]byte, iteraciones)
	writeBuffers := make([][]byte, iteraciones)

	for i := 0; i < iteraciones; i++ {
		keyName := fmt.Sprintf("pagina_%d", i)
		keys[i] = NewUUIDSheedBytes([]byte(keyName)) 
	
		bufferContent := fmt.Sprintf("%s %d", textWrite, i)
		
		writeBuffers[i] = []byte(bufferContent)
	}

	type chunk struct{ start, end int }
	chunks := make([]chunk, numWorkers)
	base := iteraciones / numWorkers
	rem := iteraciones % numWorkers
	current := 0
	for i := 0; i < numWorkers; i++ {
		c := base
		if i < rem {
			c++
		}
		chunks[i] = chunk{start: current, end: current + c}
		current += c
	}

	var writeErrors atomic.Int32
	var readErrors atomic.Int32
	var totalBytesLeidos atomic.Int64
	var wg sync.WaitGroup

	// ==========================================
	// 1. MEDIMOS ESCRITURA CONCURRENTE
	// ==========================================
	startGunWrite := make(chan struct{})

	for i := 0; i < numWorkers; i++ {
		wg.Add(1)
		go func(c chunk) {
			defer wg.Done()
			<-startGunWrite // El worker se congela aquí

			for j := c.start; j < c.end; j++ {
				// Badger usa transacciones (Update) para escribir
				err := db.Update(func(txn *badger.Txn) error {
					// keys[j][:] convierte [32]byte a []byte
					return txn.Set(keys[j][:], writeBuffers[j])
				})
				
				if err != nil {
					writeErrors.Add(1)
					println(err.Error())
				}
			}
		}(chunks[i])
	}

	startWrite := time.Now()
	close(startGunWrite)                
	wg.Wait()                           
	timeWrite := time.Since(startWrite) 

	// ==========================================
	// 2. MEDIMOS LECTURA CONCURRENTE
	// ==========================================
	startGunRead := make(chan struct{})

	for i := 0; i < numWorkers; i++ {
		wg.Add(1)
		go func(wID int, c chunk) {
			defer wg.Done()
			localReadBuffer := make([]byte, 128)

			<-startGunRead // Se congela esperando la señal

			for j := c.start; j < c.end; j++ {
				var bytesRead int
				// Badger usa transacciones de solo lectura (View)
				err := db.View(func(txn *badger.Txn) error {
					item, err := txn.Get(keys[j][:])
					if err != nil {
						return err // Puede ser badger.ErrKeyNotFound
					}
					
					// item.Value permite acceder al valor sin hacer allocations extras de memoria
					return item.Value(func(val []byte) error {
						bytesRead = copy(localReadBuffer, val)
						return nil
					})
				})

				if err != nil {
					readErrors.Add(1)
				} else {
					totalBytesLeidos.Add(int64(bytesRead))
				}
			}
		}(i, chunks[i])
	}

	startRead := time.Now()
	close(startGunRead) 
	wg.Wait()
	timeRead := time.Since(startRead) 

	// ==========================================
	// COMPROBACIÓN FINAL DE INTEGRIDAD (Fuera de tiempo)
	// ==========================================
	testBuffer := make([]byte, 128)
	lastIdx := iteraciones - 1
	var nReadTest int

	_ = db.View(func(txn *badger.Txn) error {
		item, err := txn.Get(keys[lastIdx][:])
		if err == nil {
			_ = item.Value(func(val []byte) error {
				nReadTest = copy(testBuffer, val)
				return nil
			})
		}
		return err
	})

	// ==========================================
	// RESULTADOS
	// ==========================================
	fmt.Println("=== BADGER BENCHMARK (", numWorkers, " WORKERS | ", iteraciones, " ITERACIONES ) ===")

	fmt.Printf("BADGER WRITE Total: %v\n", timeWrite)
	fmt.Printf("BADGER WRITE Medio/Op: %v\n", timeWrite/time.Duration(iteraciones))
	if wErrs := writeErrors.Load(); wErrs > 0 {
		fmt.Printf("BADGER Errores en Write: %d\n", wErrs)
	}

	fmt.Println("---------------------------------")

	fmt.Printf("BADGER READ Total:  %v\n", timeRead)
	fmt.Printf("BADGER READ Medio/Op: %v\n", timeRead/time.Duration(iteraciones))
	if rErrs := readErrors.Load(); rErrs > 0 {
		fmt.Printf("BADGER Errores en Read: %d\n", rErrs)
	}

	fmt.Println("---------------------------------")
	fmt.Printf("BADGER Total Bytes Leídos: %d\n", totalBytesLeidos.Load())

	if nReadTest > 0 {
		fmt.Printf("BADGER Prueba final (Lectura de la última llave [%d]): '%s'\n", lastIdx, string(testBuffer[:nReadTest]))
	}
}