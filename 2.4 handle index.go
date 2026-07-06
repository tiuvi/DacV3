package dacV3

import (
	"errors"
)

/*
secuencia
paginacion
items
hash
*/
func (sfDacV3 *dacV3) newIndex(offset int64, sizePagination uint32) (err error) {

	idIndex, index := sfDacV3.indexLocation.New()

	index.mu.Lock()
	defer index.mu.Unlock()

	idBuffer, buf := sfDacV3.indexBuffer.addBufferArena()

	bufIndex := indexBuffer(buf)

	bufIndex.SetSizePagination(sizePagination)

	bufIndex.SetSequence(1)

	bufIndex.SetCheckSum()

	index.idLocationBuffer = idBuffer

	index.offset = offset

	_, err = sfDacV3.file.WriteAt(buf[0:4096], offset)
	if err != nil {
		return err
	}

	indexChan, exists := sfDacV3.indexPools[sizePagination]
	if !exists {
		return errors.New("Ese tamaño de indice no existe")
	}

	indexChan <- idIndex

	return
}

// ESta funcion actua dentro de un bloqueo
func (sfDacV3 *dacV3) updateIndex(index *Index) (err error) {

	buf := sfDacV3.indexBuffer.getBufferArena(index.idLocationBuffer)

	bufIndex := indexBuffer(buf[0:4096])

	secuence := bufIndex.GetSequence() + 1

	// Asignamos la nueva secuencia al buffer
	bufIndex.SetSequence(secuence)

	// Calculamos el nuevo hash basado en los datos actuales del bloque y lo guardamos
	bufIndex.SetCheckSum()

	// Escribimos en el disco exactamente el Bloque 1
	_, err = sfDacV3.file.WriteAt(buf[0:4096], index.offset)
	if err != nil {
		return err
	}

	return nil
}

var errIndexCorrupt = errors.New("indice corrupto")

// Inicar los indices y todas las paginas de indice
func (sfDacV3 *dacV3) InitIndexPage(offset int64) (index *Index, err error) {

	idIndex, index := sfDacV3.indexLocation.New()

	index.mu.Lock()
	defer index.mu.Unlock()

	idBuffer, buf := sfDacV3.indexBuffer.addBufferArena()

	index.idLocationBuffer = idBuffer

	index.idLocationIndex = idIndex

	sfDacV3.ReadAt(buf, offset)
	if err != nil {
		return
	}

	indexBuffer1 := indexBuffer(buf[0:4096])

	_, sequence1, hash1 := indexBuffer1.GetMetadata()

	indexBuffer2 := indexBuffer(buf[4096:8192])

	_, sequence2, hash2 := indexBuffer2.GetMetadata()

	if sequence1 > sequence2 {

		//Bloque correcto
		if hash1 == indexBuffer1.CalCheckSum() {

			index.offset = offset

			return index, nil
		}
	}

	//Apartir de aqui si el bloque no es correcto
	if hash2 == indexBuffer2.CalCheckSum() {

		index.offset = offset

		return index, nil
	}

	return nil, nil
}

// Lee un indice ya creado
func (sfDacV3 *dacV3) ReadIndexPage(id int64) (index *Index, buf []byte) {

	index = sfDacV3.indexLocation.Get(id)

	buf = sfDacV3.indexBuffer.getBufferArena(index.idLocationBuffer)

	return index, buf
}
