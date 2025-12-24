#ifndef MAIN_H_
#define MAIN_H_

#include "utils/utils.h"
#include "utils/servidor.h"
#include "utils/serializacion.h"


t_list* workers_conectados;
pthread_t hilo_conexion_workers;
pthread_t hilo_peticiones_worker;
char* superblock_path, *hashes_path, *bitmap_path, *files_path, *blocks_path; 
uint32_t fs_size, block_size; 


void load_storage_config(char* path);
void liberar_storage();

typedef struct {
    char* puerto_escucha;
    bool fresh_start;
    char* punto_montaje;
    int retardo_operacion;
    int retardo_acceso_bloque;
    char* log_level;
} t_storage_config;

typedef struct{
    int id;
    int conexion_worker;    
} t_worker;

char* devolver_path_bloque_fisico_libre();

#endif
