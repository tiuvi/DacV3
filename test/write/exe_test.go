package test // Asegúrate de que este sea el mismo 'package' que tiene TestDacV3_CrashEnergy

import (
	"testing"
)

func TestAllCrash(t *testing.T) {
	// Definimos los parámetros de cada prueba
	type testCase struct {
		name          string // Nombre descriptivo del subtest
		crashType     string // El ID numérico (vacío si no aplica)
		crashTypeLine string // La línea (vacío si no aplica)
		interaction   string // "100" o "1000"
	}

	interactionValue := "1000"

	tests := []testCase{
		/*
				// --- CRASH TYPE 1 ---
				{"Reservo_indice_en_memoria_linea_0", "1", "0", interactionValue},
				{"Reservo_indice_en_memoria_linea_10", "1", "10", interactionValue},
				{"Reservo_indice_en_memoria_linea_60", "1", "60", interactionValue},
				{"Reservo_indice_en_memoria_linea_250", "1", "250", interactionValue},
				{"Reservo_indice_en_memoria_linea_500", "1", "500", interactionValue},

				// --- CRASH TYPE 2 ---
				{"Antes_cambiar_indices_aumento_tamaño_linea_0", "2", "0", interactionValue},
				{"Antes_cambiar_indices_aumento_tamaño_linea_10", "2", "10", interactionValue},
				{"Antes_cambiar_indices_aumento_tamaño_linea_60", "2", "60", interactionValue},
				{"Antes_cambiar_indices_aumento_tamaño_linea_250", "2", "250", interactionValue},
				{"Antes_cambiar_indices_aumento_tamaño_linea_500", "2", "500", interactionValue},

				// --- CRASH TYPE 3 ---
				{"Crash_intercambio_de_indices_linea_0", "3", "0", interactionValue},
				{"Crash_intercambio_de_indices_linea_10", "3", "10", interactionValue},
				{"Crash_intercambio_de_indices_linea_60", "3", "60", interactionValue},
				{"Crash_intercambio_de_indices_linea_250", "3", "250", interactionValue},
				{"Crash_intercambio_de_indices_linea_500", "3", "500", interactionValue},

				// --- CRASH TYPE 4 ---
				{"Crash_wal_checksum_datos_medias_linea_0", "4", "0", interactionValue},
				{"Crash_wal_checksum_datos_medias_linea_10", "4", "10", interactionValue},
				{"Crash_wal_checksum_datos_medias_linea_60", "4", "60", interactionValue},
				{"Crash_wal_checksum_datos_medias_linea_250", "4", "250", interactionValue},
				{"Crash_wal_checksum_datos_medias_linea_500", "4", "500", interactionValue},

				// --- CRASH TYPE 5 ---
				{"Crash_escribiendo_wall_a_medias_linea_0", "5", "0", interactionValue},
				{"Crash_escribiendo_wall_a_medias_linea_10", "5", "10", interactionValue},
				{"Crash_escribiendo_wall_a_medias_linea_60", "5", "60", interactionValue},
				{"Crash_escribiendo_wall_a_medias_linea_250", "5", "250", interactionValue},
				{"Crash_escribiendo_wall_a_medias_linea_500", "5", "500", interactionValue},

				// --- CRASH TYPE 6 ---
				{"Crash_indice_de_wal_incorrecto_linea_0", "6", "0", interactionValue},
				{"Crash_indice_de_wal_incorrecto_linea_10", "6", "10", interactionValue},
				{"Crash_indice_de_wal_incorrecto_linea_60", "6", "60", interactionValue},
				{"Crash_indice_de_wal_incorrecto_linea_250", "6", "250", interactionValue},
				{"Crash_indice_de_wal_incorrecto_linea_500", "6", "500", interactionValue},

				// --- CRASH TYPE 7 ---
				{"Corrupto_indice_nuevo_pagina_superior_linea_0", "7", "0", interactionValue},
				{"Corrupto_indice_nuevo_pagina_superior_linea_10", "7", "10", interactionValue},
				{"Corrupto_indice_nuevo_pagina_superior_linea_60", "7", "60", interactionValue},
				{"Corrupto_indice_nuevo_pagina_superior_linea_250", "7", "250", interactionValue},
				{"Corrupto_indice_nuevo_pagina_superior_linea_500", "7", "500", interactionValue},

				// --- CRASH TYPE 8 ---
				{"Corrupto_indice_viejo_pagina_superior_linea_0", "8", "0", interactionValue},
				{"Corrupto_indice_viejo_pagina_superior_linea_10", "8", "10", interactionValue},
				{"Corrupto_indice_viejo_pagina_superior_linea_60", "8", "60", interactionValue},
				{"Corrupto_indice_viejo_pagina_superior_linea_250", "8", "250", interactionValue},
				{"Corrupto_indice_viejo_pagina_superior_linea_500", "8", "500", interactionValue},

				// --- CRASH TYPE 9 ---
				{"Corrupcion_durante_escritura_linea_0", "9", "0", interactionValue},
				{"Corrupcion_durante_escritura_linea_10", "9", "10", interactionValue},
				{"Corrupcion_durante_escritura_linea_60", "9", "60", interactionValue},
				{"Corrupcion_durante_escritura_linea_250", "9", "250", interactionValue},
				{"Corrupcion_durante_escritura_linea_500", "9", "500", interactionValue},

				// --- CRASH TYPE 10 ---
				{"Corrupcion_en_el_wall_linea_0", "10", "0", interactionValue},
				{"Corrupcion_en_el_wall_linea_10", "10", "10", interactionValue},
				{"Corrupcion_en_el_wall_linea_60", "10", "60", interactionValue},
				{"Corrupcion_en_el_wall_linea_250", "10", "250", interactionValue},
				{"Corrupcion_en_el_wall_linea_500", "10", "500", interactionValue},

				// --- CRASH TYPE 11 ---
				{"Corrupcion_despues_de_escribir_linea_0", "11", "0", interactionValue},
				{"Corrupcion_despues_de_escribir_linea_10", "11", "10", interactionValue},
				{"Corrupcion_despues_de_escribir_linea_60", "11", "60", interactionValue},
				{"Corrupcion_despues_de_escribir_linea_250", "11", "250", interactionValue},
				{"Corrupcion_despues_de_escribir_linea_500", "11", "500", interactionValue},


			// --- CRASH TYPE 12 ---
			{"Corrupcion_indice_despues_datos_bien_linea_0", "12", "0", interactionValue},
			{"Corrupcion_indice_despues_datos_bien_linea_10", "12", "10", interactionValue},
			{"Corrupcion_indice_despues_datos_bien_linea_60", "12", "60", interactionValue},
			{"Corrupcion_indice_despues_datos_bien_linea_250", "12", "250", interactionValue},
			{"Corrupcion_indice_despues_datos_bien_linea_500", "12", "500", interactionValue},

				// --- CRASH TYPE 13 ---
				{"Corrupcion_despues_wal_ubicacion_pag_linea_0", "13", "0", interactionValue},
				{"Corrupcion_despues_wal_ubicacion_pag_linea_10", "13", "10", interactionValue},
				{"Corrupcion_despues_wal_ubicacion_pag_linea_60", "13", "60", interactionValue},
				{"Corrupcion_despues_wal_ubicacion_pag_linea_250", "13", "250", interactionValue},
				{"Corrupcion_despues_wal_ubicacion_pag_linea_500", "13", "500", interactionValue},

				// --- CRASH TYPE 14 ---
				{"Corrupcion_escribiendo_en_wall_linea_0", "14", "0", interactionValue},
				{"Corrupcion_escribiendo_en_wall_linea_10", "14", "10", interactionValue},
				{"Corrupcion_escribiendo_en_wall_linea_60", "14", "60", interactionValue},
				{"Corrupcion_escribiendo_en_wall_linea_250", "14", "250", interactionValue},
				{"Corrupcion_escribiendo_en_wall_linea_500", "14", "500", interactionValue},

				// --- CRASH TYPE 15 ---
				{"Corrupcion_despues_de_escribirse_linea_0", "15", "0", interactionValue},
				{"Corrupcion_despues_de_escribirse_linea_10", "15", "10", interactionValue},
				{"Corrupcion_despues_de_escribirse_linea_60", "15", "60", interactionValue},
				{"Corrupcion_despues_de_escribirse_linea_250", "15", "250", interactionValue},
				{"Corrupcion_despues_de_escribirse_linea_500", "15", "500", interactionValue},

				// --- CRASH TYPE 16 ---
				{"Corrupcion_indice_despues_escribir_wal_linea_0", "16", "0", interactionValue},
				{"Corrupcion_indice_despues_escribir_wal_linea_10", "16", "10", interactionValue},
				{"Corrupcion_indice_despues_escribir_wal_linea_60", "16", "60", interactionValue},
				{"Corrupcion_indice_despues_escribir_wal_linea_250", "16", "250", interactionValue},
				{"Corrupcion_indice_despues_escribir_wal_linea_500", "16", "500", interactionValue},

				// --- CRASH TYPE 17 ---
				{"Crash_error_sincronizacion_final_linea_0", "17", "0", interactionValue},
				{"Crash_error_sincronizacion_final_linea_10", "17", "10", interactionValue},
				{"Crash_error_sincronizacion_final_linea_60", "17", "60", interactionValue},
				{"Crash_error_sincronizacion_final_linea_250", "17", "250", interactionValue},
				{"Crash_error_sincronizacion_final_linea_500", "17", "500", interactionValue},

				// --- CRASH TYPE 18 ---
				{"Crash_cierre_inesperado_db_linea_0", "18", "0", interactionValue},
				{"Crash_cierre_inesperado_db_linea_10", "18", "10", interactionValue},
				{"Crash_cierre_inesperado_db_linea_60", "18", "60", interactionValue},
				{"Crash_cierre_inesperado_db_linea_250", "18", "250", interactionValue},
				{"Crash_cierre_inesperado_db_linea_500", "18", "500", interactionValue},
		*/

		{"Corrupto_indice_viejo_pagina_superior_linea_500", "8", "500", interactionValue},
		{"Corrupcion_durante_escritura_linea_500", "9", "500", interactionValue},
		{"Corrupcion_en_el_wall_linea_500", "10", "500", interactionValue},
		{"Corrupcion_despues_de_escribir_linea_500", "11", "500", interactionValue},
		{"Corrupcion_indice_despues_datos_bien_linea_500", "12", "500", interactionValue},
		
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			// 1. Configuramos el directorio y la interacción para este subtest aislando el entorno
			t.Setenv("dir", "/mnt/ramdisk/")
			t.Setenv("interaction", tc.interaction)

			if tc.crashType != "" {
				t.Setenv("crashType", tc.crashType)
			}
			if tc.crashTypeLine != "" {
				t.Setenv("crashTypeLine", tc.crashTypeLine)
			}

			// 2. Ejecutamos la función de test original pasándole el contexto actual (t)
			TestDacV3_CrashEnergy(t)
		})
		//time.Sleep(5 * time.Second)
	}
}
