package dacV3

const (

	//NEWPAGE -> Escritura cuando una pagina pasa a otra superior, siempre direct , ej: 4k -> 16k -> 32k

	//ESCRITURAS
	CrashNewPageBeforeWrite = iota + 1
	CrashNewPageAfterWrite

	//CORRUPCION ESCRITURAS
	CrashNewPageWhileWriteData
	CrashNewPageWhileWriteWalIndex
	CrashNewPageWhileWriteWalData

	//INDICES
	CrashNewPageWhileWriteNewIndex
	CrashNewPageAfterNewIndexWrite
	CrashNewPageWhileWriteOldIndex

	//PAGE DIRECT-> Escrituras en una misma pagina de forma directa, si se supera el tamaño del archivo se puede
	//escribir de forma directa en cualquier bloque multiplo de 4096.

	//ESCRITURAS
	CrashPageDirectAfterWrite

	//CORRUPCION ESCRITURAS
	CrashPageDirectWhileWriteData
	CrashPageDirectWhileWriteWalIndex
	CrashPageDirectWhileWriteWalData

	//INDICES
	CrashPageDirectWhileWriteIndex

	//PAGE WAL -> Escrituras en la misma pagina atraves del wal, primero se escribe en el wal despues en la
	//direccion real

	//ESCRITURAS
	CrashPageWalAfterWrite

	//CORRUPCION ESCRITURAS
	CrashPageWalWhileWriteData
	CrashPageWalWhileWriteWalData
	CrashPageWalWhileWriteWalIndex

	//INDICES
	CrashPageWalWhileWriteIndex

	//CORRUPCION DE DATOS

	//ESCRITURA DE DATOS OFFSET REAL
	CrashWriteUnsafeCorrupt

	//ESCRITURA INDICES EN EL WAL
	CrashWriteWallIndexCorrupt

	//ESCRITURA DATOS EN EL WAL
	CrashWriteWallCorrupt

	//ESCRITURA INDICES DE PAGINAS
	CrashWriteIndexCorrupt
)

var TestCrashEnergy int64

var TestCrashEnergiChan = make(chan bool)


// Mapa inverso: ID numérico -> Nombre legible del Crash
var CrashNames = map[int]string{
	// NEWPAGE
	CrashNewPageBeforeWrite:        "CrashNewPageBeforeWrite",
	CrashNewPageAfterWrite:         "CrashNewPageAfterWrite",
	CrashNewPageWhileWriteData:     "CrashNewPageWhileWriteData",
	CrashNewPageWhileWriteWalIndex: "CrashNewPageWhileWriteWalIndex",
	CrashNewPageWhileWriteWalData:  "CrashNewPageWhileWriteWalData",
	CrashNewPageWhileWriteNewIndex: "CrashNewPageWhileWriteNewIndex",
	CrashNewPageAfterNewIndexWrite: "CrashNewPageAfterNewIndexWrite",
	CrashNewPageWhileWriteOldIndex: "CrashNewPageWhileWriteOldIndex",

	// PAGE DIRECT
	CrashPageDirectAfterWrite:        "CrashPageDirectAfterWrite",
	CrashPageDirectWhileWriteData:    "CrashPageDirectWhileWriteData",
	CrashPageDirectWhileWriteWalIndex: "CrashPageDirectWhileWriteWalIndex",
	CrashPageDirectWhileWriteWalData:  "CrashPageDirectWhileWriteWalData",
	CrashPageDirectWhileWriteIndex:   "CrashPageDirectWhileWriteIndex",

	// PAGE WAL
	CrashPageWalAfterWrite:        "CrashPageWalAfterWrite",
	CrashPageWalWhileWriteData:    "CrashPageWalWhileWriteData",
	CrashPageWalWhileWriteWalData: "CrashPageWalWhileWriteWalData",
	CrashPageWalWhileWriteWalIndex: "CrashPageWalWhileWriteWalIndex",
	CrashPageWalWhileWriteIndex:   "CrashPageWalWhileWriteIndex",

	// CORRUPCION DE DATOS
	CrashWriteUnsafeCorrupt:    "CrashWriteUnsafeCorrupt",
	CrashWriteWallIndexCorrupt: "CrashWriteWallIndexCorrupt",
	CrashWriteWallCorrupt:      "CrashWriteWallCorrupt",
	CrashWriteIndexCorrupt:     "CrashWriteIndexCorrupt",
}

// CrashName devuelve el nombre del crash o un string genérico si no existe
func CrashName(crashID int) string {

	if name, ok := CrashNames[crashID]; ok {
		return name
	}

	return "UnknownCrash"
}
