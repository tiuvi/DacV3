package test

import (
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"testing"

	"dacV3" // Importación estricta solicitada
)

func TestDacV3_SingleGrowingPage_LineByLine(t *testing.T) {

	dir := filepath.Join(os.Getenv("dir"), "dacV3.db")

	interactionEnv := os.Getenv("interaction")
	interaction, err := strconv.ParseInt(interactionEnv, 10, 64)
	if err != nil || interaction == 0 {
		interaction = 10 // Por defecto
	}

	config := dacV3.NewDacV3Options(dir, true, 1)

	db := dacV3.InitDacV3(config)

	// Creamos UNA ÚNICA KEY
	key := dacV3.NewUUIDSheedBytes([]byte("single_growing_page_strict"))

	var fullContent string
	totalLines := int(interaction)

	t.Logf("Iniciando prueba de estrés: %d líneas escritas (Appends) y validadas UNA POR UNA...", totalLines)

	localReadBuffer := make([]byte, 1024*1024) // 1MB buffer inicial

	// VARIABLE MÁGICA: El offset dinámico
	var currentOffset int64 = 0

	for i := 0; i < totalLines; i++ {

		// 1. Generamos LA NUEVA LÍNEA
		newLine := fmt.Sprintf("Esta es la linea consecutiva numero %d para ver como crece el archivo\n", i)

		// OJO: Solo convertimos a bytes la LÍNEA NUEVA, no todo el archivo
		contentBytes := []byte(newLine)

		// 2. ESCRITURA EN DISCO (Usando el offset actual para hacer "Append")
		// (Asegúrate de castear currentOffset si tu WritePage pide uint32 u otro tipo, ej: uint32(currentOffset))
		err := db.WritePage(key, contentBytes, currentOffset)
		if err != nil {
			t.Fatalf("Fallo crítico en WritePage al escribir la línea %d en el offset %d: %v", i, currentOffset, err)
		}

		// 3. ACTUALIZAMOS TRACKERS EN MEMORIA
		fullContent += newLine

		currentOffset += int64(len(contentBytes)) // Movemos el cursor para la próxima línea

		// 4. LECTURA INMEDIATA PARA VERIFICAR CORRUPCIÓN
		if len(fullContent) > len(localReadBuffer) {
			localReadBuffer = make([]byte, len(fullContent)+4096)
		}

		// Leemos TODO desde el offset 0 para asegurarnos de que la concatenación interna en disco funcionó
		n, err := db.ReadPage(key, localReadBuffer, 0)
		if err != nil {
			t.Fatalf("Fallo crítico en ReadPage tras escribir la línea %d: %v", i, err)
		}

		// 5. COMPARACIÓN EXACTA BYTE POR BYTE
		actual := string(localReadBuffer[:n])

		if actual != fullContent {
			// Si el Swap falló, cortó datos o sobrescribió mal por el offset, lo atrapamos aquí
			t.Fatalf("\n🚨 CORRUPCIÓN DETECTADA en la línea %d 🚨\n"+
				"Tamaño Esperado: %d bytes | Tamaño Leído: %d bytes\n"+
				"Última línea intentada en offset %d: '%s'",
				i, len(fullContent), n, currentOffset-int64(len(contentBytes)), newLine[:len(newLine)-1])
		}

		// Progreso
		if (i+1)%100 == 0 {
			t.Logf("Progreso: %d/%d líneas (Appends) exitosas (Tamaño actual en disco: %d bytes)", i+1, totalLines, len(fullContent))
		}
	}

	t.Logf("==== TEST DE ESTRÉS COMPLETADO CON ÉXITO ====")
	t.Logf("Tamaño del payload interno: %d bytes", len(fullContent))

}
