#ifndef MEMORIA_H_
#define MEMORIA_H_


#include <utils/utils.h>


extern pthread_mutex_t mutex_memoria;
extern pthread_mutex_t mutex_log;
// configuración de la memoria interna
typedef enum{
   LRU,
   CLOCK_M
} t_algoritmo;
//Estructura de una página
typedef struct{
   uint32_t numero_pagina;
   uint32_t marco;
   bool presente;
   bool modificado;
   bool uso;
   t_temporal *tiempo; //LRU
}t_pagina;


//Estructura de una tabla de páginas por File:Tag
typedef struct{
   char* file;
   char* tag;
   t_list* paginas;
}t_tabla_paginas;


typedef struct{
   t_tabla_paginas* tabla;
   t_pagina* pagina;
} t_pagina_tabla; // Para el algoritmo de clock
typedef struct{
   void* memoria;
   uint32_t tam_memoria;
   uint32_t tam_pagina;
   uint32_t cant_marcos;
   bool* marcos_ocupados; //bitmap
   t_dictionary* tabla_paginas;
   uint32_t puntero_clock;
   t_algoritmo algoritmo; // LRU o CLOCK-M
   t_pagina_tabla** tabla_inversa_marcos;
}t_memoria_interna;
typedef struct{
   bool fallo_pagina;
   bool requiere_flush;
   t_pagina* pagina;
   uint32_t marco_victima;
} t_resultado_memoria;


extern t_log* logger;
extern t_memoria_interna* memoria;


//Crea la memoría recibiendo el tamaño de página y el argoritmo definidos por config
t_memoria_interna* crear_memoria(uint32_t tam_memoria, uint32_t tam_pagina, char* algoritmo);
//Destruye y libera todos los elementos de la estructura de memoria
void destruir_memoria(t_memoria_interna* memoria);
//Funcion utilizada para destruir las tablas de la memoria
void destruir_tabla (void* elemento);
//Realiza una lectura de la memoria segun file, tag y nro de pagina, devolviendo si hubo fallo de página, si se requiere flush, la página victima y el marco donde se encuentra
t_resultado_memoria leer_memoria(t_memoria_interna* memoria, char* file, char* tag, int nro_pagina);
//Realiza una escritura en la memoria segun file, tag y nro de pagina y devuelve si hubo fallo de página, si se requiere flush, la página victima y el marco donde se encuentra
t_resultado_memoria escribir_memoria(t_memoria_interna* memoria, char* file, char* tag, uint32_t nro_pagina, void* dato, uint32_t size);
//Devuelve la cantidad de páginas presentes en una tabla
uint32_t paginas_presente(t_tabla_paginas* tabla);
//Carga los datos de la página solicitada en el marco indicado
void cargar_pagina(t_memoria_interna* memoria, char* file, char* tag, int nro_pagina, int marco, void* datos, uint32_t size);
int buscar_marco_libre(t_memoria_interna* memoria);
//Devuelve el marco segúl el algoritmo LRU o -1 si la memoria está llena con una página de cada tabla
int buscar_marco_lru(t_memoria_interna* memoria);
//Devuelve el marco segúl el algoritmo CLOCK-M o -1 si la memoria está llena con una página de cada tabla
int buscar_marco_clok_m(t_memoria_interna* memoria);
void mostrar_estado_pagina(t_memoria_interna* memoria);
//Devuelve una tabla según file y tag existente o crea una
t_tabla_paginas* traer_tabla(t_memoria_interna* memoria, char* file, char* tag);
//Devuelve la página buscada por número o NULL
t_pagina* buscar_pagina(t_tabla_paginas* tabla, uint32_t numero_pagina);
//Devuelve una estructura con la página buscada por marco y la tabla a la que pertenece o NULL
t_pagina_tabla* buscar_pagina_por_marco(t_memoria_interna* memoria, int marco);
//convierte el char* del algoritmo de reemplazo por un enum del tipo t_algoritmo
t_algoritmo string_a_enum(char* str);
char* generar_clave_tabla(char* file, char* tag);




#endif
