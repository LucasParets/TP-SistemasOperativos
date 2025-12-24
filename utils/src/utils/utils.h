#ifndef UTILS_H_
#define UTILS_H_

#include <stdio.h> // printf()
#include <stdlib.h> // exit()
#include <string.h> // memset()
#include <unistd.h> // fork(), close()
#include <stdint.h>
#include <dirent.h>
#include <math.h> 

#include <sys/socket.h> // socket(), accept(), connect()
#include <sys/types.h> // socket(), accept(), connect()
#include <sys/stat.h>
#include <sys/types.h>
#include <sys/mman.h>
#include <netdb.h> // getaddrinfo()

#include <semaphore.h>
#include <commons/config.h>
#include <commons/log.h>
#include <commons/string.h>
#include <commons/collections/list.h>
#include <commons/collections/dictionary.h>
#include <commons/temporal.h>
#include <commons/crypto.h>
#include <commons/txt.h>
#include <commons/bitarray.h>
#include <commons/collections/queue.h>

#include <readline/readline.h>
#include <assert.h>
#include <pthread.h> // pthread_create()
#include <readline/readline.h>
#include <errno.h>
#include <sys/stat.h>
#include <sys/types.h> 
#include <semaphore.h>
#include <pthread.h>

#define ERROR -1
#define OK     0

typedef char* String;

typedef enum {
    QC_CON_MASTER = 0,
    WORKER_CON_MASTER,
    WORKER_CON_STORAGE,
    HANDSHAKE_ERROR
} conexion_t; // Tipo de conexión especifico entre modulos

typedef enum {
    NADA = 0,
    NUMERO,
    TEXTO,
    MANDAR_ID,
    TAM_BLOQUE_STORAGE, //worker le pide a storage el tamaño de bloque
    EJECUTAR_QUERY, //master le manda a worker que ejecute una query
    DESALOJAR_QUERY, //master le manda a worker que desaloje una query
    OP_CREATE, //worker le manda a storage la operacion create
    OP_EXITOSA, //storage le responde a worker que la operacion fue exitosa
    OP_TRUNCATE, //worker le manda a storage la operacion truncate
    OP_TAG, //worker le manda a storage la operacion tag
    OP_COMMIT, //worker le manda a storage la operacion commit
    OP_FLUSH, //worker le manda a storage la operacion flush
    OP_DELETE, //worker le manda a storage la operacion delete
    ESCRIBIR_BLOQUE, //worker le manda a storage que escriba un bloque
    LEER_BLOQUE, //worker le manda a storage que lea un bloque
    OP_END, //worker le manda a master la operacion end
    RESULTADO_LECTURA, //worker le manda a master el resultado de una lectura
    DESALOJO_WORKER,
    DESCONEXION, // El worker le dice a storage que desaloja
    FILE_TAG_INEXISTENTE,
    FILE_TAG_PREEXISTENTE,
    ESPACIO_INSUFICIENTE,
    ESCRITURA_NO_PERMITIDA,
    LECTURA_FUERA_DE_LIMITE,
    ESCRITURA_FUERA_DE_LIMITE, //worker le manda a master el pc luego de un desalojo de query
    MANDAR_QUERY,
    LECTURA_ARCHIVO,
    FINALIZACION_QUERY,    
    DEVOLVER_PC,
    REANUDAR_QUERY,
    FINALIZAR_QUERY_CON_ERROR,
    FINALIZACION_QUERY_DESCONEXION_WORKER,
    DESCONEXION_WORKER, 
    LIMITE_FISICO_EXCEDIDO,
    OPERACION_PROHIBIDA
} tipo_op;

typedef enum {
    CREATE,
    TRUNCATE,
    WRITE,
    READ,
    TAG,
    COMMIT,
    FLUSH,
    DELETE,
    END
} instruccion;



typedef struct {
    instruccion tipo;
    char* parametro1;  
    char* parametro2;  
    char* parametro3;   
} t_instruccion;

t_log* iniciar_logger(char* path, char* name, bool is_active_console, t_log_level level);
t_config* iniciar_config(char* path);
void error_exit(char *message);
instruccion string_a_instruccion(char* comando);

#endif

