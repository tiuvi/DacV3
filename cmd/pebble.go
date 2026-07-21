package main

import (
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	. "dacV3" // Tu librería
	"github.com/cockroachdb/pebble"
)

/* 
  NOTA: Para abrir Pebble en tu main debes hacerlo así:
  
  dbPebble, err := pebble.Open("ruta/a/pebble", &pebble.Options{})
  if err != nil { ... }
  defer dbPebble.Close()
*/

func runTestPebble(db *pebble.DB) {
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
				// Usamos pebble.Sync para forzar el Fsync a disco y que sea justa la comparativa
				err := db.Set(keys[j][:], writeBuffers[j], pebble.Sync)
				
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
				
				// Pebble devuelve el valor y un "Closer" que hay que cerrar para no fugar memoria
				val, closer, err := db.Get(keys[j][:])
				
				if err != nil {
					readErrors.Add(1)
				} else {
					bytesRead = copy(localReadBuffer, val)
					totalBytesLeidos.Add(int64(bytesRead))
					
					// ¡MUY IMPORTANTE cerrar el closer de Pebble!
					closer.Close()
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

	valTest, closerTest, errTest := db.Get(keys[lastIdx][:])
	if errTest == nil {
		nReadTest = copy(testBuffer, valTest)
		closerTest.Close()
	}

	// ==========================================
	// RESULTADOS
	// ==========================================
	fmt.Println("=== PEBBLE BENCHMARK (", numWorkers, " WORKERS | ", iteraciones, " ITERACIONES ) ===")

	fmt.Printf("PEBBLE WRITE Total: %v\n", timeWrite)
	fmt.Printf("PEBBLE WRITE Medio/Op: %v\n", timeWrite/time.Duration(iteraciones))
	if wErrs := writeErrors.Load(); wErrs > 0 {
		fmt.Printf("PEBBLE Errores en Write: %d\n", wErrs)
	}

	fmt.Println("---------------------------------")

	fmt.Printf("PEBBLE READ Total:  %v\n", timeRead)
	fmt.Printf("PEBBLE READ Medio/Op: %v\n", timeRead/time.Duration(iteraciones))
	if rErrs := readErrors.Load(); rErrs > 0 {
		fmt.Printf("PEBBLE Errores en Read: %d\n", rErrs)
	}

	fmt.Println("---------------------------------")
	fmt.Printf("PEBBLE Total Bytes Leídos: %d\n", totalBytesLeidos.Load())

	if nReadTest > 0 {
		fmt.Printf("PEBBLE Prueba final (Lectura de la última llave [%d]): '%s'\n", lastIdx, string(testBuffer[:nReadTest]))
	}
}