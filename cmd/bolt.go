package main

import (
	"fmt"
	"sync"
	"sync/atomic"
	"time"
	. "dacV3"
	"go.etcd.io/bbolt"
)

func runTestBolt(db *bbolt.DB) {
	// ==========================================
	// 0. PREPARACIÓN DE BOLT (Buckets)
	// ==========================================
	bucketName := []byte("BenchmarkBucket")
	
	// Nos aseguramos de que el bucket exista antes de empezar el test
	// para no penalizar el tiempo de escritura midiendo la creación del bucket.
	err := db.Update(func(tx *bbolt.Tx) error {
		_, err := tx.CreateBucketIfNotExists(bucketName)
		return err
	})
	if err != nil {
		panic(fmt.Sprintf("Error creando bucket de Bolt: %v", err))
	}

	// ==========================================
	// PREPARACIÓN DE DATOS Y WORKERS (NO se mide)
	// ==========================================


	keys := make([][32]byte, iteraciones)
	writeBuffers := make([][]byte, iteraciones)

	for i := 0; i < iteraciones; i++ {
		keyName := fmt.Sprintf("pagina_%d", i)
		keys[i] = NewUUIDSheedBytes([]byte(keyName)) // Asumo que tienes esta func en tu scope
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
				// Bolt usa transacciones de actualización para escribir
				err := db.Update(func(tx *bbolt.Tx) error {
					b := tx.Bucket(bucketName)
					// keys[j][:] convierte el [32]byte a []byte, que es lo que pide Bolt
					return b.Put(keys[j][:], writeBuffers[j])
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
				// Bolt usa transacciones de solo lectura (View)
				err := db.View(func(tx *bbolt.Tx) error {
					b := tx.Bucket(bucketName)
					val := b.Get(keys[j][:])
					if val == nil {
						return fmt.Errorf("not found")
					}
					// Copiamos el dato al buffer local para simular tu ReadPage
					bytesRead = copy(localReadBuffer, val)
					return nil
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

	_ = db.View(func(tx *bbolt.Tx) error {
		b := tx.Bucket(bucketName)
		val := b.Get(keys[lastIdx][:])
		if val != nil {
			nReadTest = copy(testBuffer, val)
		}
		return nil
	})

	// ==========================================
	// RESULTADOS
	// ==========================================
	fmt.Println("=== BOLT BENCHMARK (", numWorkers, " WORKERS | ", iteraciones, " ITERACIONES ) ===")

	fmt.Printf("BOLT WRITE Total: %v\n", timeWrite)
	fmt.Printf("BOLT WRITE Medio/Op: %v\n", timeWrite/time.Duration(iteraciones))
	if wErrs := writeErrors.Load(); wErrs > 0 {
		fmt.Printf("BOLT Errores en Write: %d\n", wErrs)
	}

	fmt.Println("---------------------------------")

	fmt.Printf("BOLT READ Total:  %v\n", timeRead)
	fmt.Printf("BOLT READ Medio/Op: %v\n", timeRead/time.Duration(iteraciones))
	if rErrs := readErrors.Load(); rErrs > 0 {
		fmt.Printf("BOLT Errores en Read: %d\n", rErrs)
	}

	fmt.Println("---------------------------------")
	fmt.Printf("BOLT Total Bytes Leídos: %d\n", totalBytesLeidos.Load())

	if nReadTest > 0 {
		fmt.Printf("BOLT Prueba final (Lectura de la última llave [%d]): '%s'\n", lastIdx, string(testBuffer[:nReadTest]))
	}
}