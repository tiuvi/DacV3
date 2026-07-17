package main

import (
	. "dacV3"
	"fmt"
	"sync"
	"sync/atomic"
	"time"
)

/*
/media/franky/tiuviweb/go/bin/go mod init dacv3Main
/media/franky/tiuviweb/go/bin/go mod tidy

/media/franky/tiuviweb/go/bin/go build -o dacV3Run main.go
chmod +x dacV3Run
./dacV3Run

/media/franky/tiuviweb/go/bin/go build -o dacV3Run main.go && chmod +x dacV3Run && ./dacV3Run

/media/franky/tiuviweb/go/bin/go run main.go
*/

func main() {

	ramDisk := "/mnt/ramdisk/dacV3.db"
	diskTest := "/mnt/disk/dbDisk.db"

	var diskPath = ""
	if false {
		diskPath = ramDisk
	} else {
		diskPath = diskTest
	}

	multiplierChan := uint32(10)
	config := DacV3Options{
		DacRoute:        diskPath,
		SizeIndexMaster: 4096,              //multiplos de 4096
		MaxReserveSize:  1024 * 1024 * 100, // 100 MB
		SsdNIopsMili:    50,

		NBuffersAvailableIndexSearch:     8,
		NChanAvaibleIndexSearch:          8 * multiplierChan,
		NBuffersAvailableIndexSearchData: MaxSubIndexPerIndex * 8,

		NBuffersAvailableIndex: 23,
		SupportedSizes: []SizeConfig{
			{
				Size:                4096,
				IndexSizeChan:       16 * multiplierChan,
				NBuffersAvaibleData: MaxSubIndexPerIndex * 16,
			},
			{
				Size:                16384,
				IndexSizeChan:       4 * multiplierChan,
				NBuffersAvaibleData: MaxSubIndexPerIndex * 4,
			},
			{
				Size:                32768,
				IndexSizeChan:       2 * multiplierChan,
				NBuffersAvaibleData: MaxSubIndexPerIndex * 2,
			},
			{
				Size:                65536,
				IndexSizeChan:       1 * multiplierChan,
				NBuffersAvaibleData: MaxSubIndexPerIndex,
			},
		},
		NWorkers:  32,
		QueueSize: 1024,
	}

	// Creamos la instancia pasando las opciones (NO se mide)
	db := InitDacV3(config)

	// ==========================================
	// PREPARACIÓN DE DATOS Y WORKERS (NO se mide)
	// ==========================================
	const iteraciones = 100
	const numWorkers = 128 // Cantidad de hilos simultáneos

	keys := make([][32]byte, iteraciones)
	writeBuffers := make([][]byte, iteraciones)

	for i := 0; i < iteraciones; i++ {
		keyName := fmt.Sprintf("pagina_%d", i)
		keys[i] = NewUUIDSheedBytes([]byte(keyName))

		bufferContent := fmt.Sprintf("hola mundo %d", i)
		writeBuffers[i] = []byte(bufferContent)
	}

	// Repartimos el trabajo entre los workers (ej: Worker 1 hace del 0 al 12, etc.)
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

	// Contadores atómicos (súper rápidos y seguros para concurrencia)
	var writeErrors atomic.Int32
	var readErrors atomic.Int32
	var totalBytesLeidos atomic.Int64

	var wg sync.WaitGroup

	// ==========================================
	// 1. MEDIMOS ESCRITURA CONCURRENTE
	// ==========================================
	startGunWrite := make(chan struct{}) // El "Pistoletazo de salida"

	for i := 0; i < numWorkers; i++ {
		wg.Add(1)
		go func(c chunk) {
			defer wg.Done()
			<-startGunWrite // El worker se congela aquí hasta que cerremos el canal

			for j := c.start; j < c.end; j++ {
				err := db.WritePage(keys[j], writeBuffers[j], 0)
				if err != nil {
					writeErrors.Add(1)
				}
			}
		}(chunks[i])
	}

	startWrite := time.Now()
	close(startGunWrite)                // ¡LIBERAMOS A TODOS LOS WORKERS AL MISMO TIEMPO!
	wg.Wait()                           // Esperamos que todos terminen
	timeWrite := time.Since(startWrite) // ¡Corte de tiempo limpio!

	// ==========================================
	// 2. MEDIMOS LECTURA CONCURRENTE
	// ==========================================
	startGunRead := make(chan struct{})

	for i := 0; i < numWorkers; i++ {
		wg.Add(1)
		go func(wID int, c chunk) {
			defer wg.Done()

			// MUY IMPORTANTE: Cada worker necesita su propio buffer para evitar Data Races
			localReadBuffer := make([]byte, 128)

			<-startGunRead // Se congela esperando la señal

			for j := c.start; j < c.end; j++ {
				nRead, err := db.ReadPage(keys[j], localReadBuffer, 0)
				if err != nil {
					readErrors.Add(1)
				}
				totalBytesLeidos.Add(int64(nRead))
			}
		}(i, chunks[i])
	}

	startRead := time.Now()
	close(startGunRead) // ¡LIBERAMOS LAS LECTURAS AL MISMO TIEMPO!
	wg.Wait()
	timeRead := time.Since(startRead) // ¡Corte de tiempo limpio!

	// ==========================================
	// COMPROBACIÓN FINAL DE INTEGRIDAD (Fuera de tiempo)
	// ==========================================
	// Hacemos una lectura individual de la última página generada para confirmar
	// que los datos se guardaron correctamente durante el caos concurrente.
	testBuffer := make([]byte, 128)
	lastIdx := iteraciones - 1
	nReadTest, _ := db.ReadPage(keys[lastIdx], testBuffer, 0)

	// ==========================================
	// RESULTADOS
	// ==========================================
	fmt.Println("=== BENCHMARK CONCURRENTE (", numWorkers, " WORKERS | ", iteraciones, " ITERACIONES ) ===")

	fmt.Printf("WRITE Total: %v\n", timeWrite)
	fmt.Printf("WRITE Medio/Op: %v\n", timeWrite/time.Duration(iteraciones))
	if wErrs := writeErrors.Load(); wErrs > 0 {
		fmt.Printf("Errores en Write: %d\n", wErrs)
	}

	fmt.Println("---------------------------------")

	fmt.Printf("READ Total:  %v\n", timeRead)
	fmt.Printf("READ Medio/Op: %v\n", timeRead/time.Duration(iteraciones))
	if rErrs := readErrors.Load(); rErrs > 0 {
		fmt.Printf("Errores en Read: %d\n", rErrs)
	}

	fmt.Println("---------------------------------")
	fmt.Printf("Total Bytes Leídos: %d\n", totalBytesLeidos.Load())

	if nReadTest > 0 {
		fmt.Printf("Prueba final (Lectura de la última llave [%d]): '%s'\n", lastIdx, string(testBuffer[:nReadTest]))
	}
}
