#ifndef MAIN_H_
#define MAIN_H_

#include "utils/utils.h"
#include "utils/cliente.h"
#include "utils/serializacion.h"
#include "memoria.h"

typedef struct {
    char* ip_master;
    char* puerto_master;
    char* ip_storage;
    char* puerto_storage;
    int tam_memoria;
    int retardo_memoria;
    char* algoritmo_reemplazo;
    char* path_scripts;
    char* log_level;
} t_worker_config;


extern t_config* config;
extern t_worker_config worker_config;
extern t_log* logger;
extern t_log* logger_de_comunicacion;
extern int conexion_master, conexion_storage, id_worker;
extern uint32_t pc;

extern sem_t sem_ejecutar_query;
extern pthread_mutex_t mutex_desalojo;
extern pthread_mutex_t mutex_resultado_operacion;
extern pthread_mutex_t mutex_memoria;
extern pthread_mutex_t mutex_log;

extern bool desalojo_pendiente;
extern uint32_t pc_desalojo;
extern uint32_t query_id_actual;

extern t_memoria_interna* memoria;

extern uint32_t resultado_operacion;

void load_worker_config(char* path);
void liberar_worker();
void iniciar();
void atender_master();
int obtener_tam_bloque_storage(int conexion);

#endif