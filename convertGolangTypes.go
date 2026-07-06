package main

import (
	"bytes"
	"encoding/binary"
	"encoding/gob"
	"encoding/json"
	"math"
	"unicode/utf8"
)

var dacBoolTrue []byte = []byte("t")

var dacBoolFalse []byte = []byte("f")

func BytesToBool(buffer []byte) bool {

	return len(buffer) > 0 && buffer[0] == dacBoolTrue[0]
}
func BoolToBytes(value bool) []byte {

	if value {
		return dacBoolTrue
	}

	return dacBoolFalse
}

func BytesToRune(buffer []byte) []rune {

	return bytes.Runes(buffer)
}

func RunesToBytes(runes []rune) []byte {

	buffer := make([]byte, 0, len(runes)*utf8.UTFMax)

	var b [utf8.UTFMax]byte

	for _, r := range runes {
		n := utf8.EncodeRune(b[:], r)
		buffer = append(buffer, b[:n]...)
	}
	return buffer
}

func BytesToUint64(buffer []byte) uint64 {

	if len(buffer) != 8 {
		return 0
	}

	return binary.LittleEndian.Uint64(buffer)

}

func Uint64ToBytes(value uint64) []byte {

	buffer := make([]byte, 8)

	binary.LittleEndian.PutUint64(buffer, value)

	return buffer
}

func BytesToUint32(buffer []byte) uint32 {

	if len(buffer) != 4 {
		return 0
	}

	return binary.LittleEndian.Uint32(buffer)

}

func Uint32ToBytes(value uint32) []byte {

	buffer := make([]byte, 4)

	binary.LittleEndian.PutUint32(buffer, value)

	return buffer
}

func BytesToUint16(buffer []byte) uint16 {

	if len(buffer) != 2 {
		return 0
	}

	return binary.LittleEndian.Uint16(buffer)

}

func Uint16ToBytes(value uint16) []byte {

	buffer := make([]byte, 2)

	binary.LittleEndian.PutUint16(buffer, value)

	return buffer
}

func BytesToUint8(buffer []byte) uint8 {

	if len(buffer) == 1 {
		return 0
	}

	return (buffer)[0]
}

func Uint8ToBytes(value uint8) []byte {

	return []byte{value}
}

func Int64ToBytes(value int64) []byte {

	return Uint64ToBytes(uint64(value))

}

func BytesToInt64(buffer []byte) int64 {

	return int64(BytesToUint64(buffer))
}

func Int32ToBytes(value int32) []byte {

	return Uint32ToBytes(uint32(value))

}

func BytesToInt32(buffer []byte) int32 {

	return int32(BytesToUint32(buffer))
}

func IntToBytes(value int) []byte {

	return Uint32ToBytes(uint32(value))

}

func BytesToInt(buffer []byte) int {

	return int(BytesToInt32(buffer))
}

func BytesToInt16(buffer []byte) int16 {

	return int16(BytesToUint16(buffer))
}

func Int16ToBytes(value int16) []byte {

	return Uint16ToBytes(uint16(value))

}

func BytesToInt8(buffer []byte) int8 {

	return int8(BytesToUint8(buffer))
}

func Int8ToBytes(value int8) []byte {

	return Uint8ToBytes(uint8(value))

}

func BytesToFloat64(buffer []byte) float64 {

	if len(buffer) != 8 {
		return 0
	}

	bits := binary.LittleEndian.Uint64(buffer)

	return math.Float64frombits(bits)

}

func Float64ToBytes(value float64) []byte {

	buffer := make([]byte, 8)

	bits := math.Float64bits(value)

	binary.LittleEndian.PutUint64(buffer, bits)

	return buffer

}
func BytesToFloat64Slice(buf []byte) []float64 {

	if len(buf) == 0 {
		return []float64{}
	}

	n := len(buf) / 8

	out := make([]float64, n)

	for i := range n {

		off := i * 8

		bits := binary.LittleEndian.Uint64(buf[off : off+8])

		out[i] = math.Float64frombits(bits)

	}

	return out
}

func Float64SliceToBytes(value []float64) []byte {

	n := len(value)

	buf := make([]byte, n*8) // tamaño exacto: 8 bytes por float64

	for i, v := range value {

		bits := math.Float64bits(v)

		off := i * 8

		binary.LittleEndian.PutUint64(buf[off:off+8], bits)
	}

	return buf
}

func BytesToFloat32(buffer []byte) float32 {

	if len(buffer) != 4 {
		return 0
	}

	bits := binary.LittleEndian.Uint32(buffer)

	return math.Float32frombits(bits)
}

func Float32ToBytes(value float32) []byte {

	buffer := make([]byte, 4)

	bits := math.Float32bits(value)

	binary.LittleEndian.PutUint32(buffer, bits)

	return buffer

}

func BytesToFloat32Array(buffer []byte) []float32 {

	if len(buffer)%4 != 0 {
		return nil // el buffer no es múltiplo de 4, error
	}

	n := len(buffer) / 4

	result := make([]float32, n)

	for i := 0; i < n; i++ {
		start := i * 4
		result[i] = BytesToFloat32(buffer[start : start+4])
	}

	return result
}

func Float32ArrayToBytes(values []float32) []byte {

	totalLen := 4 * len(values) // cada float32 ocupa 4 bytes

	buf := make([]byte, 0, totalLen)

	for _, v := range values {
		buf = append(buf, Float32ToBytes(v)...)
	}

	return buf
}

func BytesToComplex128(buffer []byte) complex128 {

	if len(buffer) != 16 {
		return 0
	}

	real := math.Float64frombits(binary.LittleEndian.Uint64(buffer))

	imag := math.Float64frombits(binary.LittleEndian.Uint64((buffer)[8:]))

	return complex(real, imag)

}

func Complex128ToBytes(value complex128) []byte {

	buffer := make([]byte, 16)

	binary.LittleEndian.PutUint64(buffer, math.Float64bits(real(value)))

	binary.LittleEndian.PutUint64(buffer[8:], math.Float64bits(imag(value)))

	return buffer
}

func BytesToComplex64(buffer []byte) complex64 {

	if len(buffer) != 8 {
		return 0
	}

	real := math.Float32frombits(binary.LittleEndian.Uint32(buffer))

	imag := math.Float32frombits(binary.LittleEndian.Uint32((buffer)[4:]))

	return complex(real, imag)

}

func Complex64ToBytes(value complex64) []byte {

	buffer := make([]byte, 8)

	binary.LittleEndian.PutUint32(buffer, math.Float32bits(real(value)))

	binary.LittleEndian.PutUint32(buffer[4:], math.Float32bits(imag(value)))

	return buffer
}

func TypesGolangToBytes(valueIn interface{}) ([]byte, error) {

	buffer := bytes.Buffer{}

	// Creamos un encoder que escribirá en el buffer
	encType := gob.NewEncoder(&buffer)

	// Codificamos el valor en el buffer
	err := encType.Encode(valueIn)
	if err != nil {
		return nil, err
	}

	bufferBytes := buffer.Bytes()

	bufferBytes = append(bufferBytes, 255)

	return bufferBytes, nil
}

func BytesToTypesGolang(buffer []byte, outRef interface{}) error {

	if len(buffer) == 0 {
		return nil
	}

	modifiedBuffer := buffer

	if len(buffer) > 0 {
		modifiedBuffer = buffer[:len(buffer)-1]
	}

	// Creamos un decoder que leerá desde el buffer
	decType := gob.NewDecoder(bytes.NewReader(modifiedBuffer))

	// Decodificamos el valor en la estructura out
	return decType.Decode(outRef)
}

func TypesGolangToJsonBytes(valueIn interface{}) ([]byte, error) {

	jsonBytes, err := json.Marshal(valueIn)
	if err != nil {
		return nil, err
	}

	return jsonBytes, nil
}

func JsonBytesToTypesGolang(buffer []byte, outRef interface{}) error {

	if len(buffer) == 0 {
		return nil
	}

	err := json.Unmarshal(buffer, outRef)
	if err != nil {
		return err
	}
	return nil
}
