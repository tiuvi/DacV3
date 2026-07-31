package test

import (
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"strings"
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

	// NUEVO: Obtener la línea exacta donde queremos el crash
	crashTypeLineEnv := os.Getenv("crashTypeLine")
	crashTypeLine := -1 // Usamos -1 como indicador de "no definido"
	if crashTypeLineEnv != "" {
		parsedLine, err := strconv.Atoi(crashTypeLineEnv)
		if err == nil {
			crashTypeLine = parsedLine
		}
	}

	// NUEVO: Control inicial de la bandera de crash
	if crashTypeLine == -1 {
		// No se definió línea: aplicamos el crash de inmediato
		dacV3.TestCrashEnergy = crashType
	} else {
		// Se definió línea: empezamos sin crash para que avance normalmente
		dacV3.TestCrashEnergy = 0
	}

	// RESET GLOBAL DEL TARGET PARA CADA TEST
	dacV3.TestCrashTarget.Store(-1)

	config := dacV3.NewDacV3Options(dir, true, 1)
	db := dacV3.InitDacV3(config)

	key := dacV3.NewUUIDSheedBytes([]byte("single_growing_page_strict"))
	var fullContent string
	var fullContentTotal string
	var currentOffset int64 = 0
	localReadBuffer := make([]byte, 1024*1024)
	totalLines := int(interaction)

	// ====================================================================================
	// 3. ENVOLVEMOS EL BUCLE EN UNA FUNCIÓN ANÓNIMA PARA ATRAPAR EL PANIC
	// ====================================================================================
	i := 0
	isRecover := false
	func() {
		// Este defer atrapa el panic cuando explote y evita que el test se muera
		defer func() {
			if r := recover(); r != nil {

				t.Log("🔌💥RECOVER: ", crashType, dacV3.CrashName(int(crashType)), "AddCrash: ", crashTypeLine, "CrashLine: ", i, "Interaccion: ", interaction)
				isRecover = true
				// Apagamos la bandera para que la recuperación no vuelva a hacer panic
				dacV3.TestCrashEnergy = 0
			}
		}()

		// Bucle original
		for i = 0; i < totalLines; i++ {

			// NUEVO: Si definimos una línea específica, activamos el crash justo al llegar a ella
			if crashTypeLine != -1 && i == crashTypeLine {

				dacV3.TestCrashEnergy = crashType
			}

			newLine := fmt.Sprintf("Esta es la linea consecutiva numero %d ,  aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa", i)
			contentBytes := []byte(newLine)[:64]

			fullContentTotal += string(contentBytes)

			// ESCRITURA EN DISCO (Aquí es donde va a hacer panic si TestCrashEnergy está activo)
			err := db.WritePage(key, contentBytes, currentOffset)
			if err != nil {
				t.Fatalf("Fallo crítico en WritePage: %v", err)
			}

			// ¡ATENCIÓN! Si WritePage hizo panic, el programa SALTA al defer y estas líneas NO se ejecutan.
			// Por lo tanto, fullContent tendrá EXACTAMENTE el texto previo al corte de energía.
			fullContent += string(contentBytes)
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

	println("\n db clear \n")

	//Limpiamos db
	db.Clear()

	// ====================================================================================

	// 4. EL PANIC HA SIDO ATRAPADO. EL PROGRAMA CONTINÚA AQUÍ.
	// Si hubo crash, cerramos (si aplica) y reiniciamos para forzar lectura de disco.
	if crashType == 0 {

		if isRecover {
			t.Fatal("🚨 PANIC INESPERADO: Ocurrió un panic pero crashType era 0")
		}

		t.Log("==== Sin Crash (Comportamiento esperado) ====")
		return
	} else {

		if !isRecover {
			t.Fatal("🚨 FALLO DE SIMULACIÓN: Se esperaba un crash/panic pero el bucle terminó normalmente")
		}
	}

	config = dacV3.NewDacV3Options(dir, false, 1)

	dbRecovery := dacV3.InitDacV3(config)
	defer func() {
		if dbRecovery != nil {
			dbRecovery.Clear() // O dbRecovery.Close() / dbRecovery.Stop()
		}

	}()

	// 5. COMPROBACIÓN FINAL (RECOVERY VÁLIDO)
	n, err := dbRecovery.ReadPage(key, localReadBuffer, 0)
	if err != nil && len(fullContent) > 0 {
		t.Fatalf("🚨 Fallo crítico: La base de datos no pudo leer la página tras el reinicio: %v", err)
	}

	size, err := dbRecovery.Size(key)
	if err != nil {
		t.Fatalf("Fallo crítico en Size: %v", err)
	}

	actual := string(localReadBuffer[:n])

	errorFunc := func() {

		bytesUltimasLineas := 2 * 64

		// Función auxiliar interna para no repetir código
		obtenerUltimas := func(textoOriginal string) string {
			trimmed := strings.TrimRight(textoOriginal, "\x00")
			if len(trimmed) >= bytesUltimasLineas {
				return string(trimmed[len(trimmed)-bytesUltimasLineas:])
			}
			return string(trimmed)
		}

		println("Buffer actual (Raw %q): \n", fmt.Sprintf("%q", actual))
		println("Buffer fullContent (Raw %q): \n", fmt.Sprintf("%q", fullContent))

		println("Buffer actual: \n", obtenerUltimas(actual), "size: ", size, "nRead: ", n)
		println("Buffer fullContent: \n", obtenerUltimas(fullContent), "size: ", len(fullContent)) // <--- AHORA SÍ USA fullContent
		println("Buffer fullContentTotal: \n", obtenerUltimas(fullContentTotal), "size: ", len(fullContentTotal))
	}

	if actual == fullContent {
		t.Log("===== Recuperadas todas las líneas excepto la última =====", "\n\n")

		if size != int64(len(fullContent)) {
			errorFunc()
			t.Fatal("ERROR INDICE", "size: ", size, " data size: ", len(fullContent), "\n\n")
		}
		return
	}

	if actual == fullContentTotal {
		t.Log("===== Recuperadas todas las líneas =====", "\n\n")

		if size != int64(len(fullContentTotal)) {
			errorFunc()
			t.Fatal("ERROR INDICE", "size: ", size, " data size: ", len(fullContentTotal), "\n\n")
		}
		return
	}

	errorFunc()

	t.Fatal("\n\n", "🚨 CORRUPCIÓN DETECTADA 🚨", "\n\n")

}
