#include "main.h"

t_config* config;
t_qc_config qc_config;
t_log* logger;
t_log* logger_de_comunicacion;
int conexion_master;

char* enum_a_string(int op) {
    switch (op) {
        case OP_EXITOSA:
            return "EXITO";

        case FILE_TAG_INEXISTENTE:
            return "FILE_TAG_INEXISTENTE";

        case FILE_TAG_PREEXISTENTE:
            return "FILE_TAG_PREEXISTENTE";

        case ESPACIO_INSUFICIENTE:
            return "ESPACIO_INSUFICIENTE";

        case ESCRITURA_NO_PERMITIDA:
            return "ESCRITURA_NO_PERMITIDA";

        case LECTURA_FUERA_DE_LIMITE:
            return "LECTURA_FUERA_DE_LIMITE";

        case ESCRITURA_FUERA_DE_LIMITE:
            return "ESCRITURA_FUERA_DE_LIMITE";
            
        case LIMITE_FISICO_EXCEDIDO:
            return "LIMITE_FISICO_EXCEDIDO";

        case DESCONEXION_WORKER:
            return "DESCONEXION_WORKER";

        default:
            return "ERROR_DESCONOCIDO";
    }
}
            



int main(int argc, char* argv[]) {
    char* config = argv[1];
    char* query_path = argv[2];
    uint32_t prioridad = (uint32_t)atoi(argv[3]);
    printf("=== INICIANDO QUERY CONTROL ===\n");

    //iniciamos la config
    load_qc_config(config);

    //iniciamos los loggers
    logger = iniciar_logger("qc.log", "QC", 1, LOG_LEVEL_INFO);
    logger_de_comunicacion = iniciar_logger("cq_debug.log", "QC", 1, LOG_LEVEL_DEBUG);

    //Query control intenta conectarse con Master
    conexion_master = conectarse_a_modulo("MASTER", qc_config.ip_master, qc_config.puerto_master, QC_CON_MASTER, logger_de_comunicacion);

    log_info(logger, "## Conexión al Master exitosa. IP: %s, Puerto: %s", qc_config.ip_master, qc_config.puerto_master);

    int size = strlen(config) + sizeof(uint32_t) + strlen(query_path) + sizeof(uint32_t) + sizeof(prioridad);
    payload_t* payload = payload_create(size);
    paquete_t* pack = crear_paquete(MANDAR_QUERY, payload);
    payload_add_string(payload, query_path);
    payload_add(payload, &prioridad, sizeof(prioridad));
    
    if(enviar_paquete(conexion_master, pack) != OK){
        printf("fallo al enviar el paquete a memoria");
        exit(EXIT_FAILURE);
    }

    log_info(logger, "## Solicitud de ejecución de Query: %s, prioridad: %d", query_path, prioridad);
    payload_destroy(pack->payload);
    liberar_paquete(pack);
    // log_info
    //ejemplo de recepcion de datos con deserializacion:

    
    bool ejecutando = true;
    while(ejecutando){
        pack = recibir_paquete(conexion_master);
    
        if(pack == NULL) {
            log_error(logger, "SE DESCONECTO EL MODULO MASTER, SE FINALIZA QUERY...");
            exit(EXIT_FAILURE);
        }

        switch (pack->cod_op)
        {   
            case OP_EXITOSA:
            {
                uint32_t numero;
                payload_read(pack->payload, &numero, sizeof(numero));
                log_info(logger,"El numero recibido por memoria es %d", numero);
                break;
            }

            case RESULTADO_LECTURA: {
                uint32_t query_id;
                payload_read(pack->payload, &query_id, sizeof(uint32_t));

                char* file = payload_read_string(pack->payload);
                char* tag  = payload_read_string(pack->payload);

                uint32_t tamanio;
                payload_read(pack->payload, &tamanio, sizeof(uint32_t));

                void* buffer = NULL;
                if (tamanio > 0) {
                    buffer = malloc(tamanio);
                    payload_read(pack->payload, buffer, tamanio);
                }

                if (buffer != NULL) {
                    char* texto = malloc(tamanio + 1);
                    memcpy(texto, buffer, tamanio);
                    texto[tamanio] = '\0';
                    log_info(logger, "## ## Lectura realizada: File <%s:%s>, contenido: <%s>", file, tag, texto);
                    free(texto);
                } else {
                    log_info(logger, "## ## Lectura realizada: File <%s:%s>, contenido: <EMPTY>", file, tag);
                }

                free(file);
                free(tag);
                if (buffer) free(buffer);
                break;
            }

            case FINALIZACION_QUERY:
                int op;
                payload_read(pack->payload, &op, sizeof(uint32_t));
                char* motivo = enum_a_string(op);
                log_info(logger, "## Query Finalizada - <%s>", motivo);
                ejecutando = false;
                break;

            
            default:
                log_error(logger, "Operacion de memoria desconocida");
                break;
        }
        payload_destroy(pack->payload);
        liberar_paquete(pack);
    }


    liberar_qc();

    return 0;
}

void load_qc_config(char* path) {

    config = iniciar_config(path);

    if(config == NULL) {
        fprintf(stderr, "Config invalido!\n");
        exit(EXIT_FAILURE);
    }

    qc_config.ip_master = config_get_string_value(config,"IP_MASTER");
    qc_config.puerto_master = config_get_string_value(config,"PUERTO_MASTER");
    qc_config.log_level = config_get_string_value(config,"LOG_LEVEL");
}

void liberar_qc() {
    close(conexion_master);
    log_destroy(logger);
    log_destroy(logger_de_comunicacion);
    config_destroy(config);
}