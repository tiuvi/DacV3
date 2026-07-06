package main

import (
	"errors"

	"golang.org/x/sys/unix"
)

func (sf *dacV3) WriteAtSync(data []byte, offset int64) {

	_, err := globalDacV3.file.WriteAt(data, offset)
	if err != nil {

		println("ERROR FATAL - WriteAtSync - WriteAt : ", err.Error())

		return
	}

	err = unix.Fdatasync(int(globalDacV3.fd))
	if err != nil {

		println("ERROR FATAL - WriteAtSync - Fdatasync : ", err.Error())

		return
	}

}

func (sfDacV3 *dacV3) ReadAt(data []byte, offset int64) {

	_, err := sfDacV3.file.ReadAt(data, offset)
	if err != nil {

		println("ERROR FATAL - ReadAt: ", err.Error())

		return
	}

	return
}

func (sf *dacV3) WriteAt(data []byte, offset int64) {

	_, err := globalDacV3.file.WriteAt(data, offset)
	if err != nil {

		println("ERROR FATAL - WriteAt: ", err.Error())

		return
	}

}


func (sf *dacV3) ExpandSize(newSize int64) error {

	// Fast path sin bloquear (Lock-Free)
	if newSize <= sf.len.Load() {
		return nil
	}

	sf.mu.Lock()
	defer sf.mu.Unlock()

	// Doble comprobación tras adquirir el Lock
	if newSize <= sf.len.Load() {
		return nil
	}

	// mode = 0 (Sin KEEP_SIZE) para evitar actualizaciones de inodo en las escrituras posteriores
	if err := unix.Fallocate(sf.fd, 0, 0, newSize); err != nil {

		// Fallback si no está soportado (particiones antiguas)
		if !errors.Is(err, unix.EOPNOTSUPP) && !errors.Is(err, unix.ENOTSUP) {
			return err
		}

		if err := unix.Ftruncate(sf.fd, newSize); err != nil {
			return err
		}
	}

	// Actualización atómica para que los demás workers pasen por el Fast Path
	sf.len.Store(newSize)

	return nil
}