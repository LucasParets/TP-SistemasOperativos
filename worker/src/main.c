#include "main.h"
#include <memoria.h>

t_config* config;
t_worker_config worker_config;
t_log* logger;
t_log* logger_de_comunicacion;
int conexion_master, conexion_storage, id_worker;
uint32_t pc;

sem_t sem_ejecutar_query;
pthread_mutex_t mutex_desalojo;
pthread_mutex_t mutex_resultado_operacion;
pthread_mutex_t mutex_memoria;
pthread_mutex_t mutex_log;
bool desalojo_pendiente = false;
uint32_t pc_desalojo = 0;
uint32_t query_id_actual = 0;

t_memoria_interna* memoria = NULL;

uint32_t resultado_operacion;

int main(int argc, char* argv[]) {
    if(argc < 3) {
        fprintf(stderr, "Uso: %s <archivo_config> <id_worker>\n", argv[0]);
        exit(EXIT_FAILURE);
    }
    id_worker = atoi(argv[2]);
    printf("=== INICIANDO WORKER ===\n");

    pthread_mutex_init(&mutex_desalojo, NULL);
    pthread_mutex_init(&mutex_resultado_operacion, NULL);
    pthread_mutex_init(&mutex_memoria, NULL);
    pthread_mutex_init(&mutex_log, NULL);
    sem_init(&sem_ejecutar_query, 0, 0);

    load_worker_config(argv[1]);

    char log_filename[64];
    
    snprintf(log_filename, sizeof(log_filename), "worker_%d.log", id_worker);
    logger = iniciar_logger(log_filename, "WORKER", 1, LOG_LEVEL_INFO);
    logger_de_comunicacion = iniciar_logger("worker_debug.log", "WORKER", 1, LOG_LEVEL_DEBUG);

    if(mkdir(worker_config.path_scripts, 0755) == -1 && errno != EEXIST) {
        fprintf(stderr, "Error al crear el directorio %s: %s\n", 
                worker_config.path_scripts, strerror(errno));
        exit(EXIT_FAILURE);
    }
    
    conexion_storage = conectarse_a_modulo("STORAGE", worker_config.ip_storage, 
                                          worker_config.puerto_storage, 
                                          WORKER_CON_STORAGE, logger_de_comunicacion);
    payload_t* payload = payload_create(sizeof(id_worker)); 
    payload_add(payload, &id_worker, sizeof(id_worker));
    paquete_t* pack = crear_paquete(MANDAR_ID, payload);
    
    if(enviar_paquete(conexion_storage, pack) != OK){
        printf("fallo al enviar el paquete a memoria");
        exit(EXIT_FAILURE);
    }

    payload_destroy(payload);
    liberar_paquete(pack);    
    uint32_t tam_pagina = obtener_tam_bloque_storage(conexion_storage);
    conexion_master = conectarse_a_modulo("MASTER", worker_config.ip_master, 
                                         worker_config.puerto_master, 
                                         WORKER_CON_MASTER, logger_de_comunicacion);

    
    memoria = crear_memoria(worker_config.tam_memoria, tam_pagina, worker_config.algoritmo_reemplazo);

    payload = payload_create(sizeof(id_worker)); 
    payload_add(payload, &id_worker, sizeof(id_worker));
    pack = crear_paquete(MANDAR_ID, payload);

    if(enviar_paquete(conexion_master, pack) != OK){
        printf("fallo al enviar el paquete a memoria");
        exit(EXIT_FAILURE);
    }

    payload_destroy(payload);
    liberar_paquete(pack);
    iniciar();
    liberar_worker();
    return 0;
}

int obtener_tam_bloque_storage(int conexion) {
    // Enviar paquete de handshake
    payload_t* payload = payload_create(sizeof(TAM_BLOQUE_STORAGE));
    int numero = 0;
    payload_add(payload, &numero, sizeof(int));
    paquete_t* pack = crear_paquete(TAM_BLOQUE_STORAGE, payload);
    if (enviar_paquete(conexion, pack) != OK) {
        log_error(logger, "Error en handshake con Storage");
        payload_destroy(payload);
        liberar_paquete(pack);
        exit(EXIT_FAILURE);
    }
    payload_destroy(pack->payload);
    liberar_paquete(pack);

    // Recibir respuesta con tam_bloque
    paquete_t* resp = recibir_paquete(conexion);
    if (resp == NULL) {
        log_error(logger, "Error al recibir tam_bloque de Storage");
        if (resp) {
            payload_destroy(resp->payload);
            liberar_paquete(resp);
        }
        exit(EXIT_FAILURE);
    }
    uint32_t tam_bloque;
    payload_read(resp->payload, &tam_bloque, sizeof(uint32_t));
    payload_destroy(resp->payload);
    liberar_paquete(resp);
    return tam_bloque;
}

void load_worker_config(char* path) {
    config = iniciar_config(path);

    if(config == NULL) {
        fprintf(stderr, "Config invalido!\n");
        exit(EXIT_FAILURE);
    }

    worker_config.ip_master = config_get_string_value(config,"IP_MASTER");
    worker_config.puerto_master = config_get_string_value(config,"PUERTO_MASTER");
    worker_config.ip_storage = config_get_string_value(config,"IP_STORAGE");
    worker_config.puerto_storage = config_get_string_value(config,"PUERTO_STORAGE");
    worker_config.tam_memoria = config_get_int_value(config,"TAM_MEMORIA");
    worker_config.retardo_memoria = config_get_int_value(config,"RETARDO_MEMORIA");
    worker_config.algoritmo_reemplazo = config_get_string_value(config,"ALGORITMO_REEMPLAZO");
    worker_config.path_scripts = config_get_string_value(config,"PATH_SCRIPTS");
    worker_config.log_level = config_get_string_value(config,"LOG_LEVEL");
}

void liberar_worker() {
    close(conexion_master);
    close(conexion_storage);
    log_destroy(logger);
    log_destroy(logger_de_comunicacion);
    config_destroy(config);
    destruir_memoria(memoria);
}

void iniciar() {
    pthread_t escuchaMaster;
    pthread_create(&escuchaMaster, NULL, (void *)atender_master, NULL);
    pthread_join(escuchaMaster, NULL);
}

