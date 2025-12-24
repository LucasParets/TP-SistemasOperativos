#ifndef MAIN_H_
#define MAIN_H_

#include "utils/utils.h"
#include "utils/servidor.h"
#include "utils/serializacion.h"
#include <time.h> //para sacar el tiempo
#include <stdbool.h> //booleanos
#include <limits.h> //me sirve para encontrar minimos

typedef enum{
    READY,
    EXEC,
    EXIT
} estado_query_t;

typedef enum{
    LIBRE,
    OCUPADO
} estado_worker_t;
typedef struct {
    char* puerto_escucha;
    char* algoritmo_planificacion;
    int tiempo_aging;
    char* log_level;
} t_master_config;

typedef struct{
    int id;
    char* path;
    int prioridad;
    estado_query_t estado;
    int worker_asignado;
    uint32_t program_counter;  // Para manejar desalojo y reanudación
  //  time_t tiempo_en_ready;   // Para calcular aging
    bool fue_desalojada;      // Para saber si fue desalojada previamente
    pthread_t hilo_aging;  // Para que la query tenga referencia a SU hilo de aging en particular (y poder regenerarlo concurrentemente)
    int vez_en_ready; // muy importante para evitar cambio de prioridad a queries que fueron a exec y fueron desalojadas ANTES del int del aging
} t_query;

typedef struct{
    t_query* query;
    int vez_de_entrada; // para poder comparar con vez_en_ready
} t_contexto_aging;

typedef struct{
    int id;
    int conexion_qc;
    t_query* query_asociada;    
} t_qc;

typedef struct{
    int id;
    int conexion_worker;  
    estado_worker_t estado;
    int query_actual;  
} t_worker;

void load_master_config(char* path);
void liberar_master();
void destruir_query(void* query_ptr);
void* aceptar_conexiones();
t_qc* crear_referencia_a_qc(int socket);
void* atender_qc(void* s);
t_worker* crear_referencia_a_worker(int id, int socket);
void* atender_worker(void* s);
void* planificador_fifo();
void* planificador_prioridades();
//void* hilo_aging_funcion();
void* hilo_aging_individual(void* puntero_a_contexto);
t_worker* buscar_worker_libre();
t_worker* buscar_worker_con_menor_prioridad(int prioridad_nueva);
void asignar_a_worker(t_query* q, t_worker* worker_libre);
void enviar_query_a_worker(t_worker* worker, t_query* query);
void desalojar_query_de_worker(t_worker* worker, t_query* query_nueva);
//void aplicar_aging();
bool comparar_prioridades(void* a, void* b);
t_worker* buscar_worker_por_id(int id);
void cancelar_query(t_query* q);
t_query* buscar_query_por_id(int id);
void notificar_desconexion_a_qc(t_query* q);
t_qc* buscar_qc_asociado(t_query* q);
void reenviar_lectura_a_qc(t_qc* qc_destino, uint32_t query_id, char* file,char* tag, void* contenido, uint32_t tamanio);
void reenviar_error_a_qc(t_qc* qc_destino, uint32_t codigo_error);
void enviar_finalizacion_a_qc(t_qc* qc_destino);
#endif
