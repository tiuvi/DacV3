package test

import (
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"testing"

	"dacV3" // Importación estricta solicitada
)

func TestDacV3_CrashEnergy(t *testing.T) {

	dir := filepath.Join(os.Getenv("dir"), "dacV3.db")

	// 1. OBTENER VARIABLES DE ENTORNO
	interactionEnv := os.Getenv("interaction")
	interaction, err := strconv.ParseInt(interactionEnv, 10, 64)
	if err != nil || interaction == 0 {
		interaction = 10 // Por defecto
	}

	crashTypeEnv := os.Getenv("crashType")
	crashType, _ := strconv.ParseInt(crashTypeEnv, 10, 64)

	dacV3.TestCrashEnergy = crashType

	config := dacV3.NewDacV3Options(dir, true, 1)
	db := dacV3.InitDacV3(config)

	key := dacV3.NewUUIDSheedBytes([]byte("single_growing_page_strict"))
	var fullContent string
	var currentOffset int64 = 0
	localReadBuffer := make([]byte, 1024*1024)
	totalLines := int(interaction)

	t.Logf("Iniciando prueba: %d líneas... Corte de energía tipo %d", totalLines, crashType)

	// ====================================================================================
	// 3. ENVOLVEMOS EL BUCLE EN UNA FUNCIÓN ANÓNIMA PARA ATRAPAR EL PANIC
	// ====================================================================================
	func() {
		// Este defer atrapa el panic cuando explote y evita que el test se muera
		defer func() {
			if r := recover(); r != nil {
				t.Logf("💥 PANIC ATRAPADO (Corte de energía simulado exitosamente): %v", r)
				// Apagamos la bandera para que la recuperación no vuelva a hacer panic
				dacV3.TestCrashEnergy = 0
			}
		}()

		// Bucle original
		for i := 0; i < totalLines; i++ {
			newLine := fmt.Sprintf("Esta es la linea consecutiva numero %d para ver como crece el archivo\n", i)
			contentBytes := []byte(newLine)

			// ESCRITURA EN DISCO (Aquí es donde va a hacer panic si TestCrashEnergy está activo)
			err := db.WritePage(key, contentBytes, currentOffset)
			if err != nil {
				t.Fatalf("Fallo crítico en WritePage: %v", err)
			}

			// ¡ATENCIÓN! Si WritePage hizo panic, el programa SALTA al defer y estas líneas NO se ejecutan.
			// Por lo tanto, fullContent tendrá EXACTAMENTE el texto previo al corte de energía.
			fullContent += newLine
			currentOffset += int64(len(contentBytes))

			// Lectura inmediata para verificar que iba bien ANTES del crash
			if len(fullContent) > len(localReadBuffer) {
				localReadBuffer = make([]byte, len(fullContent)+4096)
			}
			n, err := db.ReadPage(key, localReadBuffer, 0)
			if err != nil {
				t.Fatalf("Fallo crítico en ReadPage tras escribir la línea %d: %v", i, err)
			}

			if string(localReadBuffer[:n]) != fullContent {
				t.Fatalf("\n🚨 CORRUPCIÓN PREVIA AL CRASH en la línea %d", i)
			}
		}
	}()
	// ====================================================================================

	// 4. EL PANIC HA SIDO ATRAPADO. EL PROGRAMA CONTINÚA AQUÍ.
	// Si hubo crash, cerramos (si aplica) y reiniciamos para forzar lectura de disco.
	if crashType != 0 {
		t.Log("🔄 Reiniciando base de datos simulando el encendido del servidor tras el corte...")

		config := dacV3.NewDacV3Options(dir, false, 1)

		dbRecovery := dacV3.InitDacV3(config)

		dbRecovery.CheckIndexPageFromHash(key)

		// 5. COMPROBACIÓN FINAL (RECOVERY VÁLIDO)
		n, err := dbRecovery.ReadPage(key, localReadBuffer, 0)
		if err != nil && len(fullContent) > 0 {
			t.Fatalf("🚨 Fallo crítico: La base de datos no pudo leer la página tras el reinicio: %v", err)
		}

		actual := string(localReadBuffer[:n])

		// Comprobamos que el archivo en disco sea EXACTAMENTE igual al estado antes de que se cortara la luz.
		// Si se sobreescribió mal, sobraron bytes o faltó el Swap, actual será distinto a fullContent.
		if actual != fullContent {
			t.Fatalf("\n🚨 CORRUPCIÓN DETECTADA TRAS RECUPERACIÓN 🚨\n"+
				"El archivo físico se corrompió con el corte de energía.\n"+
				"Tamaño Esperado: %d bytes | Tamaño Leído: %d bytes\n",
				len(fullContent), n)
		}

		t.Logf("==== TEST DE RECUPERACIÓN DE ENERGÍA COMPLETADO CON ÉXITO ====")
		t.Logf("Se recuperaron intactos %d bytes escritos antes del crash.", len(fullContent))
	} else {
		t.Logf("==== TEST DE ESTRÉS COMPLETADO CON ÉXITO (Sin Crash) ====")
		t.Logf("Tamaño del payload interno: %d bytes", len(fullContent))
	}
}
