package main

import (


	"encoding/binary"
	"encoding/hex"
	"errors"
	"math/rand/v2"

	"github.com/zeebo/blake3"
)

// 1. Generador aleatorio de String (64 caracteres)
func NewUUID() string {
	return UUIDToString(NewUUIDBytes())
}

const FieldSizeUUIDString = 64

// 2. Generador aleatorio de Bytes (32 bytes puros)
func NewUUIDBytes() [32]byte {
	var id [32]byte

	// OPTIMIZACIÓN: binary.LittleEndian.PutUint64 se compila a instrucciones de CPU
	// directas en la mayoría de arquitecturas (mucho más rápido que el for loop con shifts)
	binary.LittleEndian.PutUint64(id[0:8], rand.Uint64())
	binary.LittleEndian.PutUint64(id[8:16], rand.Uint64())
	binary.LittleEndian.PutUint64(id[16:24], rand.Uint64())
	binary.LittleEndian.PutUint64(id[24:32], rand.Uint64())

	return id
}

var chatNamespaceBytes = []byte("12345678-1234-5678-1234-567812345678")

// 5. El corazón del sistema: BLAKE3 (Seguridad absoluta de 32 bytes)
func NewUUIDSheed(key []byte) string {

	return UUIDToString(NewUUIDSheedBytes(key))
}

func NewUUIDSheedBytes(key []byte) [32]byte {

	h := blake3.New()
	h.Write(chatNamespaceBytes)
	h.Write(key)

	var id [32]byte

	// EL TRUCO ESTÁNDAR DE GO:
	// Le pasas tu array local formateado como slice de longitud cero.
	// Sum() no crea memoria nueva, simplemente "añade" los 32 bytes
	// directamente dentro de la memoria de tu array 'id'.
	h.Sum(id[:0])

	return id
}

// 3. Parsear String a Bytes (Solo acepta cadenas de 64 caracteres exactos)
func ParseUUIDBytes(s string) ([32]byte, error) {
	var id [32]byte

	if len(s) != 64 {
		return id, errors.New("invalid ID length: must be 64 characters for 32-byte ID")
	}

	_, err := hex.Decode(id[:], []byte(s))
	return id, err
}

// 4. Formatear Bytes a String (Texto plano Hexadecimal, hiper rápido)
func UUIDToString(id [32]byte) string {
	// hex.EncodeToString es nativo, está optimizado en C/Assembly por Go
	// y genera exactamente el string de 64 caracteres.
	return hex.EncodeToString(id[:])
}

var hexTable = [256]string{}

func init() {
	const hex = "0123456789abcdef"
	for i := 0; i < 256; i++ {
		hexTable[i] = string([]byte{
			hex[i>>4],
			hex[i&0x0f],
		})
	}
}

func UUIDToStringParts(id [32]byte) (shard1, shard2, uuidStr string) {

	shard1 = hexTable[id[0]]

	shard2 = hexTable[id[1]]

	// Llama a tu propia función que devuelve los 64 caracteres hexadecimales
	uuidStr = UUIDToString(id)
	return
}
