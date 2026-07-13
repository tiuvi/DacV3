package dacV3

import (
	"encoding/binary"
	"errors"
)

func (b indexBuffer) newSubIndex(hash [32]byte) (id int, found bool) {

	id, found = b.GetFirstEmptyIndex()
	if !found {
		return
	}

	b.SetIndexKept(id)

	// Obtenemos el segmento de la página destinado a los subíndices
	zoneSubIndex := b[field_subIndexInit:]

	// Calculamos el desplazamiento inicial para este subíndice en particular
	offsetIndex := id * sizeSubIndex

	// Copiamos los 32 bytes del hash en la posición calculada
	// zoneSubIndex[offsetIndex : offsetIndex+32] delimita el espacio exacto de 32 bytes
	copy(zoneSubIndex[offsetIndex:offsetIndex+32], hash[:])
	return
}

// Funciones para subindices
func (b indexBuffer) setSubIndex(hash [32]byte, id int) {

	if id > MaxSubIndexPerIndex {
		panic(errSubIndexOverFlow)
		return
	}

	b.SetIndexKept(id)

	// Obtenemos el segmento de la página destinado a los subíndices
	zoneSubIndex := b[field_subIndexInit:]

	// Calculamos el desplazamiento inicial para este subíndice en particular
	offsetIndex := id * sizeSubIndex

	// Copiamos los 32 bytes del hash en la posición calculada
	// zoneSubIndex[offsetIndex : offsetIndex+32] delimita el espacio exacto de 32 bytes
	copy(zoneSubIndex[offsetIndex:offsetIndex+32], hash[:])
}

func (b indexBuffer) unSetSubIndex(id int) {

	if id > MaxSubIndexPerIndex {
		panic(errSubIndexOverFlow)
		return
	}

	b.UnSetIndexKept(id)

	// Obtenemos el segmento de la página destinado a los subíndices
	zoneSubIndex := b[field_subIndexInit:]

	// Calculamos el desplazamiento inicial para este subíndice en particular
	offsetIndex := id * sizeSubIndex

	// Simplemente llamamos a clear sobre la porción exacta de memoria (184 bytes).
	// Go se encarga de llenar todo ese segmento con ceros de la forma más rápida posible.
	clear(zoneSubIndex[offsetIndex : offsetIndex+sizeSubIndex])
}

func (b indexBuffer) GetSubIndexHash(id int) [32]byte {

	if id > MaxSubIndexPerIndex {
		panic(errSubIndexOverFlow)
	}

	zoneSubIndex := b[field_subIndexInit:]
	offsetIndex := id * sizeSubIndex

	var hash [32]byte
	// Copiamos el segmento de 32 bytes al arreglo de tamaño fijo
	copy(hash[:], zoneSubIndex[offsetIndex+subIndex_Hash_Init:offsetIndex+subIndex_Hash_End])

	return hash
}

var errSubIndexNameOverFlow = errors.New("tamaño maximo del nombre superado")
var errSubIndexNameNotZero = errors.New("tamaño minimo del nombre no puede ser 0")

func (b indexBuffer) SetSubIndexSize(id int, size int64) {

	if id > MaxSubIndexPerIndex {
		panic(errSubIndexOverFlow)
	}

	zoneSubIndex := b[field_subIndexInit:]
	offsetIndex := id * sizeSubIndex

	// El Size ocupa desde el byte 32 al 40 relativos a este subíndice
	binary.BigEndian.PutUint64(zoneSubIndex[offsetIndex+subIndex_Size_Init:offsetIndex+subIndex_Size_End], uint64(size))
}

func (b indexBuffer) GetSubIndexSize(id int) int64 {
	if id > MaxSubIndexPerIndex {
		panic(errSubIndexOverFlow)
	}

	zoneSubIndex := b[field_subIndexInit:]
	offsetIndex := id * sizeSubIndex

	// Leemos los 8 bytes como Uint64 y lo convertimos a int64
	val := binary.BigEndian.Uint64(zoneSubIndex[offsetIndex+subIndex_Size_Init : offsetIndex+subIndex_Size_End])
	return int64(val)
}

func (b indexBuffer) SetSubIndexSequence(id int, sequence int64) { // Corregido: Sequence y el parámetro

	if id > MaxSubIndexPerIndex {
		panic(errSubIndexOverFlow)
	}

	zoneSubIndex := b[field_subIndexInit:]
	offsetIndex := id * sizeSubIndex

	// Corregido: Actualizado el comentario para que tenga sentido
	// La Secuencia ocupa los bytes correspondientes relativos a este subíndice
	binary.BigEndian.PutUint64(
		zoneSubIndex[offsetIndex+subIndexSequence_Init:offsetIndex+subIndexSequence_End],
		uint64(sequence),
	)
}

func (b indexBuffer) GetSubIndexSequence(id int) int64 { // Corregido: Sequence
	if id > MaxSubIndexPerIndex {
		panic(errSubIndexOverFlow)
	}

	zoneSubIndex := b[field_subIndexInit:]
	offsetIndex := id * sizeSubIndex

	// Leemos los 8 bytes como Uint64 y lo convertimos a int64
	val := binary.BigEndian.Uint64(
		zoneSubIndex[offsetIndex+subIndexSequence_Init : offsetIndex+subIndexSequence_End],
	)
	return int64(val)
}

func (b indexBuffer) SetSubIndexName(id int, name string) error {

	if id > MaxSubIndexPerIndex {
		panic(errSubIndexOverFlow)
	}

	if len(name) > 128 {
		return errSubIndexNameOverFlow
	}

	if len(name) == 0 {
		return errSubIndexNameNotZero
	}

	zoneSubIndex := b[field_subIndexInit:]

	offsetIndex := id * sizeSubIndex

	// Determinamos el segmento exacto del nombre (desde 56 hasta 184)
	nameZone := zoneSubIndex[offsetIndex+subIndex_Name_Init : offsetIndex+subIndex_Name_End]

	// 1. Limpiar el espacio anterior con ceros
	clear(nameZone)

	// 2. Convertir a bytes y truncar a 128 si es necesario
	nameBytes := []byte(name)

	// 3. Copiar los bytes del string al segmento
	copy(nameZone, nameBytes)

	return nil
}

func (b indexBuffer) GetSubIndexName(id int) string {
	if id > MaxSubIndexPerIndex {
		panic(errSubIndexOverFlow)
	}

	zoneSubIndex := b[field_subIndexInit:]
	offsetIndex := id * sizeSubIndex

	// Obtenemos el segmento exacto del nombre
	nameZone := zoneSubIndex[offsetIndex+subIndex_Name_Init : offsetIndex+subIndex_Name_End]

	// Buscamos la longitud real del string (hasta el primer byte con valor 0)
	length := 0
	for i := 0; i < len(nameZone); i++ {
		if nameZone[i] == 0 {
			break
		}
		length++
	}

	// Retornamos el string sin los ceros de relleno
	return string(nameZone[:length])
}
