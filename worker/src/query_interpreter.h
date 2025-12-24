#ifndef QUERY_INTERPRETER_H_
#define QUERY_INTERPRETER_H_

#include "main.h"

typedef struct {
    char* path_query;
    uint32_t pc_inicial;
    uint32_t query_id;
    FILE* archivo_query; 
} t_query_args;

extern t_log* logger;
extern t_worker_config worker_config;
extern int conexion_storage;

extern sem_t sem_ejecutar_query;
extern pthread_mutex_t mutex_desalojo;
extern pthread_mutex_t mutex_resultado_operacion;
extern pthread_mutex_t mutex_log;

extern sem_t sem_desalojo;
extern bool desalojo_pendiente;
extern uint32_t pc_desalojo;
extern uint32_t query_id_actual;

extern uint32_t resultado_operacion;

void atender_master();
void ejecutar_query(FILE* archivo, uint32_t pc_inicial, uint32_t query_id);
void ejecutar_instruccion(char* linea, uint32_t query_id);
t_instruccion* formatear_instruccion(char* instruccion_a_ejecutar);
void liberar_instruccion(t_instruccion* inst);

void instruccion_create(char* file_tag, uint32_t query_id);
void instruccion_truncate(char* file_tag, int tamanio, uint32_t query_id);
void instruccion_write(char* file_tag, int direccion, char* contenido, uint32_t query_id);
void instruccion_read(char* file_tag, int direccion, int tamanio, uint32_t query_id);
void instruccion_tag(char* origen, char* destino, uint32_t query_id);
void instruccion_commit(char* file_tag, uint32_t query_id);
void instruccion_flush(char* file_tag, uint32_t query_id);
void instruccion_delete(char* file_tag, uint32_t query_id);
void instruccion_end(uint32_t query_id);

bool parsear_file_tag(char* file_tag, char** file_name, char** tag_name);

void* ejecutar_query_void(void* arg);
void* atender_master_void(void* arg);

void flush_files_modificados(uint32_t query_id);
void flush_pagina_a_storage(char* file, char* tag, uint32_t nro_bloque, int marco, uint32_t query_id);
void* solicitar_bloque_storage(char* file, char* tag, uint32_t nro_bloque, uint32_t query_id);
void enviar_lectura_a_master(char* file, char* tag, void* contenido, uint32_t tamanio, uint32_t query_id);

void enviar_error_query_master(uint32_t query_id, uint32_t codigo_error);

#endif