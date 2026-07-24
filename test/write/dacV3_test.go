package test // Cambia esto por el nombre de tu paquete

import (
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"strconv"
	"sync"
	"sync/atomic"
	"testing"

	"dacV3" // Asegúrate de que esta importación sea correcta
)

var textWrite = `Hola mundo`

func TestDacV3_WriteConcurrentWal(t *testing.T) {

	dir := filepath.Join(os.Getenv("dir"), "dacV3.db")

	interactionEnv := os.Getenv("interaction")
	interaction, err := strconv.ParseInt(interactionEnv, 10, 64)
	if err != nil {
		t.Fatal(err)
	}

	// Inicializamos tu BD con la configuración
	config := dacV3.NewDacV3Options(dir, true, 1)
	db := dacV3.InitDacV3(config)

	keys := make([][32]byte, interaction)
	vals := make([][]byte, interaction)

	// =========================================================================
	// FUNCIÓN AUXILIAR PARA SIMULAR b.RunParallel EN UN TEST NORMAL
	// =========================================================================
	runParallelSimulator := func(operation func(idx int64)) {
		var wg sync.WaitGroup
		workers := runtime.GOMAXPROCS(0) // Usa los núcleos de la CPU por defecto
		var counter int64

		for w := 0; w < workers; w++ {
			wg.Add(1)
			go func() {
				defer wg.Done()
				for {
					// Obtenemos un índice único de forma atómica
					idx := atomic.AddInt64(&counter, 1) - 1

					// Paramos cuando procesamos todo 'interaction'
					if idx >= interaction {
						break
					}

					// Ejecutamos la lógica que le pasemos
					operation(idx)
				}
			}()
		}
		wg.Wait() // Esperamos a que todos los workers terminen
	}

	// =========================================================================
	// PREPARACIÓN INICIAL CONCURRENTE
	// =========================================================================
	t.Log("Iniciando Preparación Inicial Concurrente...")
	runParallelSimulator(func(idx int64) {
		keyName := fmt.Sprintf("pagina_%d", idx)
		
		keys[idx] = dacV3.NewUUIDSheedBytes([]byte(keyName))
		// vals guardará la SEGUNDA escritura
		vals[idx] = []byte(fmt.Sprintf("Segundo %s %d", textWrite, idx))
		
		// val es lo que escribimos PRIMERO
		val := []byte(fmt.Sprintf("Primer %s %d", textWrite, idx))
		
		err := db.WritePage(keys[idx], val, 0)
		if err != nil {
			t.Error(err)
		}
	})

	// =========================================================================
	// EJECUCIÓN PARALELA 2: LECTURAS (Verificamos la primera escritura)
	// =========================================================================
	t.Log("Iniciando Fase 2: Lecturas Concurrentes...")
	runParallelSimulator(func(idx int64) {
		localReadBuffer := make([]byte, 8192)
		n, err := db.ReadPage(keys[idx], localReadBuffer, 0) // Guardamos 'n' (bytes leídos)
		if err != nil {
			t.Error(err)
			return // Salimos de esta iteración si hubo error
		}
		
		// Regeneramos el string esperado para la primera escritura
		expected := fmt.Sprintf("Primer %s %d", textWrite, idx)
		// Cortamos el buffer usando 'n' para evitar los ceros sobrantes del buffer de 8192
		actual := string(localReadBuffer[:n])

		if actual != expected {
			t.Errorf("Fase 2 falló en índice %d. Esperado: '%s' | Obtenido: '%s'", idx, expected, actual)
		}
	})

	// =========================================================================
	// EJECUCIÓN PARALELA 1: ESCRITURAS (Actualizamos los valores)
	// =========================================================================
	t.Log("Iniciando Fase 1: Escrituras Concurrentes (Actualización)...")
	runParallelSimulator(func(idx int64) {
		err := db.WritePage(keys[idx], vals[idx], 0)
		if err != nil {
			t.Error(err)
		}
	})

	// =========================================================================
	// EJECUCIÓN PARALELA 3: LECTURAS (Verificamos la actualización)
	// =========================================================================
	t.Log("Iniciando Fase 3: Segunda ronda de Lecturas Concurrentes...")
	runParallelSimulator(func(idx int64) {
		localReadBuffer := make([]byte, 8192)
		n, err := db.ReadPage(keys[idx], localReadBuffer, 0) // Guardamos 'n' (bytes leídos)
		if err != nil {
			t.Error(err)
			return // Salimos de esta iteración si hubo error
		}

		// Lo esperado ahora es lo que guardamos en vals[idx]
		expected := string(vals[idx])
		// Volvemos a cortar el buffer leído a su tamaño real
		actual := string(localReadBuffer[:n])

		if actual != expected {
			t.Errorf("Fase 3 falló en índice %d. Esperado: '%s' | Obtenido: '%s'", idx, expected, actual)
		}
	})

	t.Log("Test finalizado exitosamente.")
}