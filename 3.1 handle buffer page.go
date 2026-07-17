package dacV3


// WriteAt escribe datos en un offset específico.
// Retorna un error si se intenta escribir fuera de los límites actuales del buffer.
func (pb *GlobalBuffer) CopyAt(offset int64, data []byte) error {

	if offset < 0 {
		return ErrNegativeOffset
	}

	// Validar que los datos no excedan el tamaño actual del buffer (no se expande)
	if int(offset)+len(data) > len(pb.buf) {
		return ErrBufferOverflow
	}

	copy(pb.buf[offset:], data)

	return nil
}

