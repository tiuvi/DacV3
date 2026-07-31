package test

import (
	"dacV3"
	"fmt"
	"runtime"
	"runtime/debug"
	"testing"
)

/*

clear && go test ./test/write -count=1 -run TestAllCrash -timeout 2h -v
/mnt/ramdisk/
/mnt/disk/
*/
func TestAllCrash(t *testing.T) {
	// Configuración de los tipos de Crash 1 - 18
	crashTypeInit := 15
	crashTypeEnd := 18

	// Configuración de las Líneas/Páginas (Dinámico)
	lineInit := 0
	lineEnd := 400
	lineStep := 1 // Cambia a 10 para ir de 10 en 10, o 100 para ir de 100 en 100

	interactionValue := "1000"

	debug.SetMemoryLimit(3 * 1024 * 1024 * 1024)

	// Iteramos sobre los tipos de crash
	for crashType := crashTypeInit; crashType <= crashTypeEnd; crashType++ {

		crashName := dacV3.CrashName(crashType)

		crashTypeStr := fmt.Sprintf("%d", crashType)

		// Iteramos sobre las líneas usando el paso configurable (lineStep)
		for line := lineInit; line <= lineEnd; line += lineStep {

			lineStr := fmt.Sprintf("%d", line)

			testName := fmt.Sprintf("\n\n%s type: %s errorLine: %s", crashName, crashTypeStr, lineStr)

			ok := t.Run(testName, func(t *testing.T) {
				t.Setenv("dir", "/mnt/ramdisk/")
				t.Setenv("interaction", interactionValue)
				t.Setenv("crashType", crashTypeStr)
				t.Setenv("crashTypeLine", lineStr)

				// Ejecutamos la función de prueba
				TestDacV3_CrashEnergy(t)
			})

			if !ok {
				t.Fatalf("\n🚨 ABORTANDO SUITE: Se detectó un fallo en la línea %s del crash %s. No se probarán más líneas.", lineStr, crashName)
				// Nota: Si solo quieres salir del bucle sin matar el test padre, usa: break
			}

			runtime.GC()
			debug.FreeOSMemory()

		}
	}
}
