package dacV3

import (
	"errors"
	"log"
	"os"

	"golang.org/x/sys/unix"
)

func (sfDacV3 *DacV3) WriteAtSync(data []byte, offset int64) {

	_, err := sfDacV3.file.WriteAt(data, offset)
	if err != nil {

		println("ERROR FATAL - WriteAtSync - WriteAt : ", err.Error())

		return
	}

	err = unix.Fdatasync(int(sfDacV3.fd))
	if err != nil {

		println("ERROR FATAL - WriteAtSync - Fdatasync : ", err.Error())

		return
	}

}

func (sfDacV3 *DacV3) ReadAt(data []byte, offset int64) {

	_, err := sfDacV3.file.ReadAt(data, offset)
	if err != nil {

		println("ERROR FATAL - ReadAt: ", err.Error())

		return
	}

	return
}

func (sfDacV3 *DacV3) WriteAt(data []byte, offset int64) {

	_, err := sfDacV3.file.WriteAt(data, offset)
	if err != nil {

		println("ERROR FATAL - WriteAt: ", err.Error())

		return
	}

}

func (sf *DacV3) ExpandSize(newSize int64) {

	// Fast path sin bloquear (Lock-Free)
	if newSize <= sf.len.Load() {
		return
	}
	println("nuevo tamaño ", newSize, " viejo tamaño ", sf.len.Load())

	// mode = 0 (Sin KEEP_SIZE) para evitar actualizaciones de inodo en las escrituras posteriores
	if err := unix.Fallocate(sf.fd, 0, 0, newSize); err != nil {

		// Fallback si no está soportado (particiones antiguas)
		if !errors.Is(err, unix.EOPNOTSUPP) && !errors.Is(err, unix.ENOTSUP) {
			log.Fatalln("ERROR ExpandSize: ", err.Error())
			return
		}

		if err := unix.Ftruncate(sf.fd, newSize); err != nil {
			log.Fatalln("ERROR ExpandSize: ", err.Error())
			return
		}
	}

	// Actualización atómica para que los demás workers pasen por el Fast Path
	sf.len.Store(newSize)

	return
}

func openFileDacV3(dacRoute string, truncate bool) *DacV3 {

	fd, err := unix.Open(dacRoute, unix.O_RDWR|unix.O_CREAT|unix.O_DIRECT, 0666)
	if err != nil {
		// Manejar el error de forma oportuna
		log.Fatalf("Error al abrir el archivo: %v", err)
	}

	if truncate {
		if err := unix.Ftruncate(fd, 0); err != nil {
			log.Fatalf("Error al truncar el archivo: %v", err)
		}
	}

	// Convertimos fd (int) a uintptr de manera explícita
	dacV3Fd := os.NewFile(uintptr(fd), dacRoute)

	size, err := dacV3Fd.Seek(0, 2)
	if err != nil {
		// Manejar el error de forma oportuna
		log.Fatalf("Error al abrir el archivo: %v", err)
	}

	sfDacV3 := &DacV3{
		file: dacV3Fd,
		fd:   fd,
	}

	sfDacV3.len.Store(size)

	return sfDacV3
}
