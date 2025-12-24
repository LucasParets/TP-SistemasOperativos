#include "query_interpreter.h"
#include "utils/serializacion.h"
#include <string.h>
#include <unistd.h>

void atender_master() {
    while(1) {
        paquete_t* pack = recibir_paquete(conexion_master);
        if(pack == NULL) {
            log_error(logger, "SE DESCONECTO EL MODULO MASTER, SALIENDO DE WORKER...");
            exit(EXIT_FAILURE);
        }

        switch(pack->cod_op) {
            case MANDAR_QUERY: {
                uint32_t pc;
                uint32_t query_id;
                payload_read(pack->payload, &query_id, sizeof(query_id));
                char* path_query = payload_read_string(pack->payload);
                payload_read(pack->payload, &pc, sizeof(pc));
                
                char ruta_completa[512];
                snprintf(ruta_completa, sizeof(ruta_completa), "%s/%s", worker_config.path_scripts, path_query);
                pthread_mutex_lock(&mutex_log);
                log_info(logger, "## Query %d: Se recibe la Query. El path de operaciones es: %s", query_id, ruta_completa);
                pthread_mutex_unlock(&mutex_log);
                FILE* archivo_query = fopen(ruta_completa, "r");
                if(archivo_query == NULL) {
                    log_error(logger, "Query %d: No se pudo abrir el archivo: %s", query_id, ruta_completa);
                    free(path_query);
                    break;
                }

                t_query_args* args = malloc(sizeof(t_query_args));
                args->path_query = path_query;
                args->pc_inicial = pc;
                args->query_id = query_id;
                args->archivo_query = archivo_query;

                pthread_t thread_query;
                pthread_create(&thread_query, NULL, ejecutar_query_void, args);
                pthread_detach(thread_query);

                break;
            }
            case DESALOJAR_QUERY: {  
                uint32_t query_id;
                payload_read(pack->payload, &query_id, sizeof(query_id));

                pthread_mutex_lock(&mutex_desalojo);
                desalojo_pendiente = true;
                query_id_actual = query_id;
                pthread_mutex_unlock(&mutex_desalojo);

                pthread_mutex_lock(&mutex_log);
                log_info(logger, "## Query %d: Desalojada por pedido del Master", query_id);
                pthread_mutex_unlock(&mutex_log);
                break;
            }
            default:
                log_warning(logger, "Tipo de paquete desconocido recibido de MASTER: %d", pack->cod_op);
                break;
            }
        payload_destroy(pack->payload);
        liberar_paquete(pack);
    }
}

// funcion wrapper para ejecutar_query en hilo
void* ejecutar_query_void(void* arg) {
    t_query_args* args = (t_query_args*)arg;

    ejecutar_query(args->archivo_query, args->pc_inicial, args->query_id);

    fclose(args->archivo_query); 
    free(args->path_query);
    free(args);
    return NULL;
}

void ejecutar_query(FILE* archivo, uint32_t pc_inicial, uint32_t query_id) {
    char linea[256];
    uint32_t pc_actual = 0;
    
    // avanza al pc inicial
    while(pc_actual < pc_inicial && fgets(linea, sizeof(linea), archivo) != NULL) {
        pc_actual++;
    }

    pthread_mutex_lock(&mutex_resultado_operacion);
    resultado_operacion = OP_EXITOSA;
    pthread_mutex_unlock(&mutex_resultado_operacion);

    while(fgets(linea, sizeof(linea), archivo) != NULL) {
        linea[strcspn(linea, "\n")] = 0;
        pthread_mutex_lock(&mutex_log);
        log_info(logger, "## Query %d: FETCH - Program Counter: %d - %s", query_id, pc_actual, linea);
        pthread_mutex_unlock(&mutex_log);
        
        ejecutar_instruccion(linea, query_id);

        pthread_mutex_lock(&mutex_log);
        log_info(logger, "## Query %d: - Instrucción realizada: %s", query_id, linea);
        pthread_mutex_unlock(&mutex_log);

        pthread_mutex_lock(&mutex_resultado_operacion);
        if(resultado_operacion != OP_EXITOSA) {
            resultado_operacion = 0; 
            pthread_mutex_unlock(&mutex_resultado_operacion);
            return;
        }
        pthread_mutex_unlock(&mutex_resultado_operacion);

        // chequea si hay desalojo pendiente despues de cada instrucción
        pthread_mutex_lock(&mutex_desalojo);
        if (desalojo_pendiente && query_id_actual == query_id) {
            pc_desalojo = pc_actual + 1; 
            flush_files_modificados(query_id);

            payload_t* payload = payload_create(sizeof(uint32_t)*2);
            payload_add(payload, &query_id, sizeof(query_id));
            payload_add(payload, &pc_desalojo, sizeof(pc_desalojo));
            paquete_t* pack = crear_paquete(DESALOJO_WORKER, payload);
            enviar_paquete(conexion_master, pack);
            payload_destroy(payload);
            liberar_paquete(pack);
            pthread_mutex_unlock(&mutex_desalojo);
            return; 
        }
        desalojo_pendiente = false;
        query_id_actual = -1;
        pthread_mutex_unlock(&mutex_desalojo);
        
        pc_actual++;
        usleep(worker_config.retardo_memoria * 1000);
    }
}

// funcion wrapper para atender_master en hilo
void* atender_master_void(void* arg) {
    atender_master();
    return NULL;
}

void ejecutar_instruccion(char* linea, uint32_t query_id) {
    t_instruccion* inst = formatear_instruccion(linea);
    
    switch(inst->tipo) {
        case CREATE:
            instruccion_create(inst->parametro1, query_id);
            break;
        case TRUNCATE:
            instruccion_truncate(inst->parametro1, atoi(inst->parametro2), query_id);
            break;
        case WRITE:
            instruccion_write(inst->parametro1, atoi(inst->parametro2), inst->parametro3, query_id);
            break;
        case READ:
            instruccion_read(inst->parametro1, atoi(inst->parametro2), atoi(inst->parametro3), query_id);
            break;
        case TAG:
            instruccion_tag(inst->parametro1, inst->parametro2, query_id);
            break;
        case COMMIT:
            instruccion_commit(inst->parametro1, query_id);
            break;
        case FLUSH:
            instruccion_flush(inst->parametro1, query_id);
            break;
        case DELETE:
            instruccion_delete(inst->parametro1, query_id);
            break;
        case END:
            instruccion_end(query_id);
            break;  
        default:
            log_error(logger, "Instrucción no reconocida");
            break;   
    }
    liberar_instruccion(inst);
}

t_instruccion* formatear_instruccion(char* instruccion_a_ejecutar) {
    t_instruccion* inst = malloc(sizeof(t_instruccion));
    inst->tipo = -1;
    inst->parametro1 = NULL;
    inst->parametro2 = NULL;
    inst->parametro3 = NULL;
    
    char* copia_instruccion = strdup(instruccion_a_ejecutar);
    
    char* token = strtok(copia_instruccion, " ");
    if(token != NULL) 
        inst->tipo = string_a_instruccion(token);
    
    token = strtok(NULL, " ");
    if(token != NULL) 
        inst->parametro1 = strdup(token);
    
    token = strtok(NULL, " ");
    if(token != NULL) 
        inst->parametro2 = strdup(token);
    
    token = strtok(NULL, " ");
    if(token != NULL) 
        inst->parametro3 = strdup(token);
    
    free(copia_instruccion);
    return inst;
}

void liberar_instruccion(t_instruccion* inst) {
    if(inst->parametro1) free(inst->parametro1);
    if(inst->parametro2) free(inst->parametro2);
    if(inst->parametro3) free(inst->parametro3);
    free(inst);
}

void instruccion_create(char* file_tag, uint32_t query_id) {
    char* nombre_archivo;
    char* nombre_etiqueta;

    if(!parsear_file_tag(file_tag, &nombre_archivo, &nombre_etiqueta)) {
        pthread_mutex_lock(&mutex_log);
        log_error(logger, "Query %d: Formato inválido de ARCHIVO:ETIQUETA en CREATE: %s", query_id, file_tag);
        pthread_mutex_unlock(&mutex_log);
        return;
    }
    uint32_t tam_payload =
          sizeof(uint32_t)                              
        + sizeof(uint32_t) + strlen(nombre_archivo) + 1 
        + sizeof(uint32_t) + strlen(nombre_etiqueta) + 1; 

    payload_t* payload = payload_create(tam_payload);

    payload_add(payload, &query_id, sizeof(uint32_t));
    payload_add_string(payload, nombre_archivo);
    payload_add_string(payload, nombre_etiqueta);

    paquete_t* paquete = crear_paquete(OP_CREATE, payload);

    if(enviar_paquete(conexion_storage, paquete) != OK) {
        pthread_mutex_lock(&mutex_log);
        log_error(logger, "Query %d: Error al enviar CREATE a Storage", query_id);
        pthread_mutex_unlock(&mutex_log);
        payload_destroy(payload);
        liberar_paquete(paquete);
        free(nombre_archivo);
        free(nombre_etiqueta);
        return;
    }

    payload_destroy(payload);
    liberar_paquete(paquete);

    paquete_t* respuesta = recibir_paquete(conexion_storage);
    if(respuesta == NULL) {
        pthread_mutex_lock(&mutex_log);
        log_error(logger, "Query %d: No se recibió respuesta de Storage para CREATE %s:%s", query_id, nombre_archivo, nombre_etiqueta);
        pthread_mutex_unlock(&mutex_log);
        free(nombre_archivo);
        free(nombre_etiqueta);
        return;
    }

    switch (respuesta->cod_op) {
        case OP_EXITOSA:
            pthread_mutex_lock(&mutex_resultado_operacion);
            resultado_operacion = OP_EXITOSA;
            pthread_mutex_unlock(&mutex_resultado_operacion);
            break;

        case FILE_TAG_PREEXISTENTE:
            enviar_error_query_master(query_id, FILE_TAG_PREEXISTENTE);
            pthread_mutex_lock(&mutex_resultado_operacion); 
            resultado_operacion = FILE_TAG_PREEXISTENTE; 
            pthread_mutex_unlock(&mutex_resultado_operacion);
            break;

        default:
            pthread_mutex_lock(&mutex_log);
            log_error(logger, "Query %d: CREATE falló para %s:%s - Código desconocido: %d", query_id, nombre_archivo, nombre_etiqueta, respuesta->cod_op);
            pthread_mutex_unlock(&mutex_log);
            break;
    }

    if (respuesta->payload) {
        payload_destroy(respuesta->payload);
    }
    liberar_paquete(respuesta);

    free(nombre_archivo);
    free(nombre_etiqueta);
}

void instruccion_truncate(char* file_tag, int tamanio, uint32_t query_id) {
    char* nombre_archivo;
    char* nombre_etiqueta;

    if (!parsear_file_tag(file_tag, &nombre_archivo, &nombre_etiqueta)) {
        pthread_mutex_lock(&mutex_log);
        log_error(logger, "Query %d: Formato inválido de ARCHIVO:ETIQUETA en TRUNCATE: %s", query_id, file_tag);
        pthread_mutex_unlock(&mutex_log);
        return;
    }

    uint32_t tam_pagina = memoria->tam_pagina;

    if (tamanio % tam_pagina != 0) {
        tamanio = ((tamanio + tam_pagina - 1) / tam_pagina) * tam_pagina; // redondeo hacia arriba
        log_warning(logger, "Query %d: TRUNCATE solicitado %u no es múltiplo de %u - se ajusta a %u",
                    query_id, tamanio, tam_pagina, tamanio);
    }


    uint32_t tam_payload =
          sizeof(uint32_t) 
        + sizeof(int)                             
        + sizeof(uint32_t) + strlen(nombre_archivo) + 1 
        + sizeof(uint32_t) + strlen(nombre_etiqueta) + 1; 

    payload_t* payload = payload_create(tam_payload);

    payload_add(payload, &query_id, sizeof(uint32_t));         
    payload_add_string(payload, nombre_archivo);               
    payload_add_string(payload, nombre_etiqueta);              
    payload_add(payload, &tamanio, sizeof(int));               

    paquete_t* paquete = crear_paquete(OP_TRUNCATE, payload);
    if (enviar_paquete(conexion_storage, paquete) != OK) {
        pthread_mutex_lock(&mutex_log);
        log_error(logger, "Query %d: Error al enviar TRUNCATE a Storage", query_id);
        pthread_mutex_unlock(&mutex_log);
        payload_destroy(payload);
        liberar_paquete(paquete);
        free(nombre_archivo);
        free(nombre_etiqueta);
        return;
    }

    payload_destroy(payload);
    liberar_paquete(paquete);

    paquete_t* respuesta = recibir_paquete(conexion_storage);
    if (respuesta == NULL) {
        pthread_mutex_lock(&mutex_log);
        log_error(logger, "Query %d: No se recibió respuesta de Storage para TRUNCATE %s:%s",
                  query_id, nombre_archivo, nombre_etiqueta);
        pthread_mutex_unlock(&mutex_log);          
        free(nombre_archivo);
        free(nombre_etiqueta);
        return;
    }

    switch (respuesta->cod_op) {

        case OP_EXITOSA:
            pthread_mutex_lock(&mutex_resultado_operacion); 
            resultado_operacion = OP_EXITOSA; 
            pthread_mutex_unlock(&mutex_resultado_operacion);
            break;

        case FILE_TAG_INEXISTENTE:
            enviar_error_query_master(query_id, FILE_TAG_INEXISTENTE);
            pthread_mutex_lock(&mutex_resultado_operacion); 
            resultado_operacion = FILE_TAG_INEXISTENTE; 
            pthread_mutex_unlock(&mutex_resultado_operacion);
            break;

        case ESCRITURA_NO_PERMITIDA:
            enviar_error_query_master(query_id, ESCRITURA_NO_PERMITIDA);
            pthread_mutex_lock(&mutex_resultado_operacion); 
            resultado_operacion = ESCRITURA_NO_PERMITIDA; 
            pthread_mutex_unlock(&mutex_resultado_operacion);
            break;

        default:
            break;
    }

    if (respuesta->payload)
        payload_destroy(respuesta->payload);

    liberar_paquete(respuesta);

    free(nombre_archivo);
    free(nombre_etiqueta);
}

void instruccion_write(char* file_tag, int direccion_base, char* contenido, uint32_t query_id) {
    char* nombre_archivo;
    char* nombre_etiqueta;
    char* file_copy = NULL;
    char* tag_copy = NULL;
    uint32_t victima_num_pagina = 0;

    if (!parsear_file_tag(file_tag, &nombre_archivo, &nombre_etiqueta)) {
        pthread_mutex_lock(&mutex_log);
        log_error(logger, "Query %d: Formato inválido de ARCHIVO:ETIQUETA en WRITE: %s", query_id, file_tag);
        pthread_mutex_unlock(&mutex_log);
        return;
    }

    uint32_t tam_pagina = memoria->tam_pagina;
    uint32_t primera_pagina = direccion_base / tam_pagina;
    uint32_t offset_inicial = direccion_base % tam_pagina;
    uint32_t bytes_a_escribir = strlen(contenido); 
    uint32_t bytes_escritos = 0;

    uint32_t pagina_actual = primera_pagina;
    uint32_t offset_actual = offset_inicial;

    // loop para escribir pagina por pagina
    while (bytes_escritos < bytes_a_escribir) {
        // intenta acceder a la pagina para verificar si esta
        t_resultado_memoria resultado = leer_memoria(memoria, nombre_archivo, nombre_etiqueta, pagina_actual);

        // si hay fallo de pagina se cargar desde Storage
        if (resultado.fallo_pagina) {
            pthread_mutex_lock(&mutex_log);
            log_info(logger, "Query %d: - Memoria Miss - File: %s - Tag: %s - Pagina: %d", query_id, nombre_archivo, nombre_etiqueta, pagina_actual);
            pthread_mutex_unlock(&mutex_log);
           
            t_pagina_tabla* victima = NULL;
            if (resultado.marco_victima != -1) {
                pthread_mutex_lock(&mutex_memoria);
                victima = buscar_pagina_por_marco(memoria, resultado.marco_victima);
                if (victima != NULL) {
                    file_copy = strdup(victima->tabla->file);
                    tag_copy = strdup(victima->tabla->tag);
                    victima_num_pagina = victima->pagina->numero_pagina;
                }
                pthread_mutex_unlock(&mutex_memoria);
            }

            if (resultado.requiere_flush) {
                if (victima != NULL) {
                    flush_pagina_a_storage(file_copy, tag_copy, victima->pagina->numero_pagina, resultado.marco_victima, query_id);
                    pthread_mutex_lock(&mutex_log);
                    log_info(logger, "Query %d: Se libera el Marco: %d perteneciente al - File: %s - Tag: %s", query_id, resultado.marco_victima, victima->tabla->file, victima->tabla->tag);
                    pthread_mutex_unlock(&mutex_log);
                    //free(victima);
                }
            }

            void* datos_pagina = solicitar_bloque_storage(nombre_archivo, nombre_etiqueta, pagina_actual, query_id);

            if (datos_pagina == NULL) {
                free(nombre_archivo);
                free(nombre_etiqueta);
                free(file_copy);
                free(tag_copy);
                return;
            }

            // determinar marco para cargar
            int marco;
            if (resultado.marco_victima != -1) {
                marco = resultado.marco_victima;
            } else {
                marco = buscar_marco_libre(memoria);
            }

            if (resultado.marco_victima != -1 && victima != NULL) {
                pthread_mutex_lock(&mutex_log);
                log_info(logger, "## Query %d: Se reemplaza la página %s:%s/%u por la %s:%s/%u", query_id, file_copy, tag_copy, victima_num_pagina, nombre_archivo, nombre_etiqueta, pagina_actual);
                pthread_mutex_unlock(&mutex_log);
            }

            free(file_copy);
            free(tag_copy);
            file_copy = NULL;
            tag_copy = NULL;

            cargar_pagina(memoria, nombre_archivo, nombre_etiqueta, pagina_actual, marco, datos_pagina, tam_pagina);
            pthread_mutex_lock(&mutex_log);
            log_info(logger, "Query %d: - Memoria Add - File: %s - Tag: %s - Pagina: %d - Marco: %d", query_id, nombre_archivo, nombre_etiqueta, pagina_actual, marco);

            log_info(logger, "Query %d: Se asigna el Marco: %d a la Página: %d perteneciente al - File: %s - Tag: %s", query_id, marco, pagina_actual, nombre_archivo, nombre_etiqueta);
            pthread_mutex_unlock(&mutex_log);
            free(datos_pagina);

            // reitenta el acceso a la pagina estando cargada
            resultado = leer_memoria(memoria, nombre_archivo, nombre_etiqueta, pagina_actual);
        }

        // si la pagina esta en memoria se escriben los datos
        if (resultado.pagina != NULL && resultado.pagina->presente) {
            uint32_t bytes_disponibles_en_pagina = tam_pagina - offset_actual;
            uint32_t bytes_a_copiar;
            if (bytes_a_escribir - bytes_escritos < bytes_disponibles_en_pagina) {
                bytes_a_copiar = bytes_a_escribir - bytes_escritos;
            } else {
                bytes_a_copiar = bytes_disponibles_en_pagina;
            }

            void* direccion_fisica = memoria->memoria + (resultado.pagina->marco * tam_pagina) + offset_actual;

            memcpy(direccion_fisica, contenido + bytes_escritos, bytes_a_copiar);

            resultado.pagina->modificado = true;
            pthread_mutex_lock(&mutex_log);
            log_info(logger, "Query %d: Acción: ESCRIBIR - Dirección Física: %ld - Valor: %.*s", query_id,
                     (long)((resultado.pagina->marco * tam_pagina) + offset_actual),
                     (int)bytes_a_copiar, (char*)(contenido + bytes_escritos));
            pthread_mutex_unlock(&mutex_log);
            usleep(worker_config.retardo_memoria * 1000);

            bytes_escritos += bytes_a_copiar;
            pagina_actual++;
            offset_actual = 0;  // resetear el offset para las siguientes paginas
        } else {
            pthread_mutex_lock(&mutex_log);
            log_error(logger, "Query %d: Error al acceder a página en memoria para WRITE", query_id);
            pthread_mutex_unlock(&mutex_log);
            free(nombre_archivo);
            free(nombre_etiqueta);
            return;
        }
    }
    free(nombre_archivo);
    free(nombre_etiqueta);
}

void instruccion_read(char* file_tag, int direccion_base, int tamanio, uint32_t query_id) {
    char* nombre_archivo;
    char* nombre_etiqueta;
    char* file_copy = NULL;
    char* tag_copy = NULL;
    uint32_t victima_num_pagina = 0;

    if (!parsear_file_tag(file_tag, &nombre_archivo, &nombre_etiqueta)) {
        pthread_mutex_lock(&mutex_log);
        log_error(logger, "Query %d: Formato inválido de ARCHIVO:ETIQUETA en READ: %s", query_id, file_tag);
        pthread_mutex_unlock(&mutex_log);
        return;
    }

    uint32_t tam_pagina = memoria->tam_pagina;
    uint32_t primera_pagina = direccion_base / tam_pagina;
    uint32_t offset_inicial = direccion_base % tam_pagina;
    uint32_t bytes_a_leer = tamanio;
    uint32_t bytes_leidos = 0;
    
    // para ir acumulando los datos que lee
    void* buffer_lectura = malloc(tamanio);

    uint32_t pagina_actual = primera_pagina;
    uint32_t offset_actual = offset_inicial;

    // lee pagina por pagina
    while (bytes_leidos < tamanio) {
        // intenta leer la pagina de memoria
        t_resultado_memoria resultado = leer_memoria(memoria, nombre_archivo, nombre_etiqueta, pagina_actual);
        
        // si hay un fallo de pagina la trae del storage
        if (resultado.fallo_pagina) {
            pthread_mutex_lock(&mutex_log);
            log_info(logger, "Query %d: - Memoria Miss - File: %s - Tag: %s - Pagina: %d", query_id, nombre_archivo, nombre_etiqueta, pagina_actual);
            pthread_mutex_unlock(&mutex_log);

            t_pagina_tabla* victima = NULL;
            if (resultado.marco_victima != -1) {
                pthread_mutex_lock(&mutex_memoria);
                victima = buscar_pagina_por_marco(memoria, resultado.marco_victima);
                if (victima != NULL) {
                    file_copy = strdup(victima->tabla->file);
                    tag_copy = strdup(victima->tabla->tag);
                    victima_num_pagina = victima->pagina->numero_pagina;
                }
                pthread_mutex_unlock(&mutex_memoria);
            }

            if (resultado.requiere_flush) {
                if (victima != NULL) {
                    flush_pagina_a_storage(file_copy, tag_copy, victima->pagina->numero_pagina, resultado.marco_victima, query_id);                    
                    pthread_mutex_lock(&mutex_log);
                    log_info(logger, "Query %d: Se libera el Marco: %d perteneciente al - File: %s - Tag: %s", query_id, resultado.marco_victima, victima->tabla->file, victima->tabla->tag);
                    pthread_mutex_unlock(&mutex_log);
                    //free(victima);
                }


            }
            
            // solicita el bloque al Storage
            void* datos_pagina = solicitar_bloque_storage(nombre_archivo, nombre_etiqueta, pagina_actual, query_id);

            if (datos_pagina == NULL) {
                free(buffer_lectura);
                free(nombre_archivo);
                free(nombre_etiqueta);
                free(file_copy);
                free(tag_copy);
                return;
            }
            
            // para ver en que marco cargar la pagina
            int marco;
            if (resultado.marco_victima != -1) {
                marco = resultado.marco_victima;
            } else {
                marco = buscar_marco_libre(memoria);
            }

            if (resultado.marco_victima != -1 && victima != NULL) {
                pthread_mutex_lock(&mutex_log);
                log_info(logger, "## Query %d: Se reemplaza la página %s:%s/%u por la %s:%s/%u", query_id, file_copy, tag_copy, victima_num_pagina, nombre_archivo, nombre_etiqueta, pagina_actual);
                pthread_mutex_unlock(&mutex_log);
            }

            free(file_copy);
            free(tag_copy);
            file_copy = NULL;
            tag_copy = NULL;
            
            // carga la pagina en memoria
            cargar_pagina(memoria, nombre_archivo, nombre_etiqueta, pagina_actual, marco, datos_pagina, tam_pagina);
            pthread_mutex_lock(&mutex_log);
            log_info(logger, "Query %d: - Memoria Add - File: %s - Tag: %s - Pagina: %d - Marco: %d", query_id, nombre_archivo, nombre_etiqueta, pagina_actual, marco);
            
            log_info(logger, "Query %d: Se asigna el Marco: %d a la Página: %d perteneciente al - File: %s - Tag: %s", query_id, marco, pagina_actual, nombre_archivo, nombre_etiqueta);
            pthread_mutex_unlock(&mutex_log);
            free(datos_pagina);
            //lee de vuelta porque ahora ya esta en memoria
            resultado = leer_memoria(memoria, nombre_archivo, nombre_etiqueta, pagina_actual);
        }
        
        // con la pagina ya en memoria lee los datos
        if (resultado.pagina != NULL && resultado.pagina->presente) {
            uint32_t bytes_disponibles_en_pagina = tam_pagina - offset_actual;
            uint32_t bytes_a_copiar;
            if (bytes_a_leer < bytes_disponibles_en_pagina) {
                bytes_a_copiar = bytes_a_leer;
            } else {
                bytes_a_copiar = bytes_disponibles_en_pagina;
            }
            
            // direccion fisica en la memoria
            void* direccion_fisica = memoria->memoria + (resultado.pagina->marco * tam_pagina) + offset_actual;
            
            // copia al buffer de lectura
            memcpy(buffer_lectura + bytes_leidos, direccion_fisica, bytes_a_copiar);
            pthread_mutex_lock(&mutex_log);
            log_info(logger, "Query %d: Acción: LEER - Dirección Física: %ld - Valor: %.*s", query_id, 
                     (long)((resultado.pagina->marco * tam_pagina) + offset_actual),
                     (int)bytes_a_copiar,
                     (char*)(buffer_lectura + bytes_leidos));
            pthread_mutex_unlock(&mutex_log);
            // aplica retardo de memoria
            usleep(worker_config.retardo_memoria * 1000);
            
            bytes_leidos += bytes_a_copiar;
            bytes_a_leer -= bytes_a_copiar;
            pagina_actual++;
            offset_actual = 0; // despues de la primera pagina el offset es 0
        } else {
            pthread_mutex_lock(&mutex_log);
            log_error(logger, "Query %d: Error al leer página en memoria", query_id);
            pthread_mutex_unlock(&mutex_log);
            free(buffer_lectura);
            free(nombre_archivo);
            free(nombre_etiqueta);
            return;
        }
    }
    
    // Enviar los datos leídos al Master
    enviar_lectura_a_master(nombre_archivo, nombre_etiqueta, buffer_lectura, tamanio, query_id);
    free(buffer_lectura);
    free(nombre_archivo);
    free(nombre_etiqueta);
}

void* solicitar_bloque_storage(char* file, char* tag, uint32_t nro_bloque, uint32_t query_id) {
    uint32_t tam_payload =
        sizeof(uint32_t)                     // query_id
        + sizeof(uint32_t) + strlen(file) + 1  // file
        + sizeof(uint32_t) + strlen(tag) + 1   // tag
        + sizeof(uint32_t);                    // nro_bloque

    payload_t* payload = payload_create(tam_payload);
    payload_add(payload, &query_id, sizeof(uint32_t));
    payload_add_string(payload, file);
    payload_add_string(payload, tag);
    payload_add(payload, &nro_bloque, sizeof(uint32_t));
    
    paquete_t* paquete = crear_paquete(LEER_BLOQUE, payload);
    if (enviar_paquete(conexion_storage, paquete) != OK) {
        pthread_mutex_lock(&mutex_log);
        log_error(logger, "Query %d: Error al solicitar bloque a Storage", query_id);
        pthread_mutex_unlock(&mutex_log);
        payload_destroy(payload);
        liberar_paquete(paquete);
        return NULL;
    }
    payload_destroy(payload);
    liberar_paquete(paquete);
    
    // recibir respuesta con el contenido del bloque
    paquete_t* respuesta = recibir_paquete(conexion_storage);

    if (respuesta == NULL) {
        pthread_mutex_lock(&mutex_log);
        log_error(logger, "Query %d: No se recibió respuesta de Storage (respuesta NULL)", query_id);
        pthread_mutex_unlock(&mutex_log);
        return NULL;
    }
    
    if (respuesta->cod_op == OP_EXITOSA) {
        uint32_t tam_bloque;
        payload_read(respuesta->payload, &tam_bloque, sizeof(uint32_t));

        void* datos = malloc(tam_bloque);
        payload_read(respuesta->payload, datos, tam_bloque);


        payload_destroy(respuesta->payload);
        liberar_paquete(respuesta);

        pthread_mutex_lock(&mutex_resultado_operacion); 
        resultado_operacion = OP_EXITOSA; 
        pthread_mutex_unlock(&mutex_resultado_operacion);
        return datos;
    } else if (respuesta->cod_op == FILE_TAG_INEXISTENTE) {

        if (respuesta->payload) payload_destroy(respuesta->payload);
        liberar_paquete(respuesta);
        enviar_error_query_master(query_id, FILE_TAG_INEXISTENTE);
        pthread_mutex_lock(&mutex_resultado_operacion); 
        resultado_operacion = FILE_TAG_INEXISTENTE; 
        pthread_mutex_unlock(&mutex_resultado_operacion);
        return NULL;
    } else if (respuesta->cod_op == LECTURA_FUERA_DE_LIMITE) {

        if (respuesta->payload) payload_destroy(respuesta->payload);
        liberar_paquete(respuesta);
        enviar_error_query_master(query_id, LECTURA_FUERA_DE_LIMITE);
        pthread_mutex_lock(&mutex_resultado_operacion); 
        resultado_operacion = LECTURA_FUERA_DE_LIMITE; 
        pthread_mutex_unlock(&mutex_resultado_operacion);
        return NULL;
    } else {
        pthread_mutex_lock(&mutex_log);
        log_error(logger, "Query %d: Storage respondio cod_op desconocido %d para %s:%s (bloque %u)", query_id, respuesta->cod_op, file, tag, nro_bloque);
        pthread_mutex_unlock(&mutex_log);
        if (respuesta->payload) payload_destroy(respuesta->payload);
        liberar_paquete(respuesta);
        return NULL;
    }
    
}

void flush_pagina_a_storage(char* file, char* tag, uint32_t nro_bloque, int marco, uint32_t query_id) {
    uint32_t tam_pagina = memoria->tam_pagina;
    void* datos = memoria->memoria + (marco * tam_pagina);

    uint32_t tam_payload =
          sizeof(uint32_t)
        + sizeof(uint32_t)  
        + tam_pagina                            
        + sizeof(uint32_t) + strlen(file) + 1 
        + sizeof(uint32_t) + strlen(tag) + 1; 

    payload_t* payload = payload_create(tam_payload);

    payload_add(payload, &query_id, sizeof(uint32_t));
    payload_add_string(payload, file);
    payload_add_string(payload, tag);
    payload_add(payload, &nro_bloque, sizeof(uint32_t));
    payload_add(payload, datos, tam_pagina);
    
    paquete_t* paquete = crear_paquete(ESCRIBIR_BLOQUE, payload);
    
    if (enviar_paquete(conexion_storage, paquete) != OK) {
        pthread_mutex_lock(&mutex_log);
        log_error(logger, "Query %d: Error al hacer flush a Storage", query_id);
        pthread_mutex_unlock(&mutex_log);
    }
    
    payload_destroy(payload);
    liberar_paquete(paquete);
    
    paquete_t* respuesta = recibir_paquete(conexion_storage);
    if (respuesta == NULL) {
        pthread_mutex_lock(&mutex_log);
        log_error(logger, "Query %d: No se recibió respuesta de Storage para ESCRIBIR_BLOQUE", query_id);
        pthread_mutex_unlock(&mutex_log); 
        return;
    }

    switch (respuesta->cod_op) {
        case OP_EXITOSA:

            pthread_mutex_lock(&mutex_resultado_operacion); 
            resultado_operacion = OP_EXITOSA; 
            pthread_mutex_unlock(&mutex_resultado_operacion);
            break;

        case FILE_TAG_INEXISTENTE:

            enviar_error_query_master(query_id, FILE_TAG_INEXISTENTE);
            pthread_mutex_lock(&mutex_resultado_operacion); 
            resultado_operacion = FILE_TAG_INEXISTENTE; 
            pthread_mutex_unlock(&mutex_resultado_operacion);
            break;

        case ESCRITURA_NO_PERMITIDA:

            enviar_error_query_master(query_id, ESCRITURA_NO_PERMITIDA);
            pthread_mutex_lock(&mutex_resultado_operacion); 
            resultado_operacion = ESCRITURA_NO_PERMITIDA; 
            pthread_mutex_unlock(&mutex_resultado_operacion);
            break;

        case ESCRITURA_FUERA_DE_LIMITE:

            enviar_error_query_master(query_id, ESCRITURA_FUERA_DE_LIMITE);
            pthread_mutex_lock(&mutex_resultado_operacion);
            resultado_operacion = ESCRITURA_FUERA_DE_LIMITE;
            pthread_mutex_unlock(&mutex_resultado_operacion);
            break;

        case ESPACIO_INSUFICIENTE:

            enviar_error_query_master(query_id, ESPACIO_INSUFICIENTE);
            pthread_mutex_lock(&mutex_resultado_operacion); 
            resultado_operacion = ESPACIO_INSUFICIENTE; 
            pthread_mutex_unlock(&mutex_resultado_operacion);
            break;

        default:
            pthread_mutex_lock(&mutex_log);    
            log_error(logger, "Query %d: Storage respondió código desconocido %d para ESCRIBIR_BLOQUE %s:%s (bloque %u)",
                      query_id, respuesta->cod_op, file, tag, nro_bloque);
            pthread_mutex_unlock(&mutex_log);
            break;
    }

    if (respuesta->payload) {
        payload_destroy(respuesta->payload);
    }
    liberar_paquete(respuesta);
}

void enviar_lectura_a_master(char* file, char* tag, void* contenido, uint32_t tamanio, uint32_t query_id) {
    uint32_t tam_payload =
          sizeof(uint32_t)
        + sizeof(uint32_t)   
        + tamanio                           
        + sizeof(uint32_t) + strlen(file) + 1 
        + sizeof(uint32_t) + strlen(tag) + 1; 

    payload_t* payload = payload_create(tam_payload);

    payload_add(payload, &query_id, sizeof(uint32_t));
    payload_add_string(payload, file);
    payload_add_string(payload, tag);
    payload_add(payload, &tamanio, sizeof(uint32_t));
    payload_add(payload, contenido, tamanio);
    
    paquete_t* paquete = crear_paquete(RESULTADO_LECTURA, payload);
    
    if (enviar_paquete(conexion_master, paquete) != OK) {
        pthread_mutex_lock(&mutex_log);
        log_error(logger, "Query %d: Error al enviar lectura a Master", query_id);
        pthread_mutex_unlock(&mutex_log);
    }
    
    payload_destroy(payload);
    liberar_paquete(paquete);
}

void instruccion_tag(char* origen, char* destino, uint32_t query_id) {
    char* nombre_archivo_origen;
    char* nombre_etiqueta_origen;
    char* nombre_archivo_destino;
    char* nombre_etiqueta_destino;

    if (!parsear_file_tag(origen, &nombre_archivo_origen, &nombre_etiqueta_origen)) {
        pthread_mutex_lock(&mutex_log);
        log_error(logger, "Query %d: Formato inválido de origen en TAG: %s", query_id, origen);
        pthread_mutex_unlock(&mutex_log);
        return;
    }

    if (!parsear_file_tag(destino, &nombre_archivo_destino, &nombre_etiqueta_destino)) {
        pthread_mutex_lock(&mutex_log);
        log_error(logger, "Query %d: Formato inválido de destino en TAG: %s", query_id, destino);
        pthread_mutex_unlock(&mutex_log);
        free(nombre_archivo_origen);
        free(nombre_etiqueta_origen);
        return;
    }

    uint32_t tam_payload =
          sizeof(uint32_t)
        + sizeof(uint32_t) + strlen(nombre_archivo_origen) + 1
        + sizeof(uint32_t) + strlen(nombre_etiqueta_origen) + 1
        + sizeof(uint32_t) + strlen(nombre_archivo_destino) + 1
        + sizeof(uint32_t) + strlen(nombre_etiqueta_destino) + 1;

    payload_t* payload = payload_create(tam_payload);

    payload_add(payload, &query_id, sizeof(uint32_t));                      
    payload_add_string(payload, nombre_archivo_origen);                    
    payload_add_string(payload, nombre_etiqueta_origen);                  
    payload_add_string(payload, nombre_archivo_destino);                 
    payload_add_string(payload, nombre_etiqueta_destino);                  

    paquete_t* paquete = crear_paquete(OP_TAG, payload);
    if (enviar_paquete(conexion_storage, paquete) != OK) {
        pthread_mutex_lock(&mutex_log);
        log_error(logger, "Query %d: Error al enviar TAG a Storage", query_id);
        pthread_mutex_unlock(&mutex_log);
        payload_destroy(payload);
        liberar_paquete(paquete);
        free(nombre_archivo_origen);
        free(nombre_etiqueta_origen);
        free(nombre_archivo_destino);
        free(nombre_etiqueta_destino);
        return;
    }

    payload_destroy(payload);
    liberar_paquete(paquete);

    paquete_t* respuesta = recibir_paquete(conexion_storage);
    if (respuesta == NULL) {
        pthread_mutex_lock(&mutex_log);
        log_error(logger, "Query %d: No se recibió respuesta de Storage para TAG", query_id);
        pthread_mutex_unlock(&mutex_log);
        free(nombre_archivo_origen);
        free(nombre_etiqueta_origen);
        free(nombre_archivo_destino);
        free(nombre_etiqueta_destino);
        return;
    }

    switch (respuesta->cod_op) {
        case OP_EXITOSA:

            pthread_mutex_lock(&mutex_resultado_operacion);
            resultado_operacion = OP_EXITOSA;
            pthread_mutex_unlock(&mutex_resultado_operacion);
            break;

        case FILE_TAG_PREEXISTENTE:

            enviar_error_query_master(query_id, FILE_TAG_PREEXISTENTE);
            pthread_mutex_lock(&mutex_resultado_operacion); 
            resultado_operacion = FILE_TAG_PREEXISTENTE; 
            pthread_mutex_unlock(&mutex_resultado_operacion);
            break;

        case FILE_TAG_INEXISTENTE:

            enviar_error_query_master(query_id, FILE_TAG_INEXISTENTE);
            pthread_mutex_lock(&mutex_resultado_operacion); 
            resultado_operacion = FILE_TAG_INEXISTENTE; 
            pthread_mutex_unlock(&mutex_resultado_operacion);
            break;

        default:
            pthread_mutex_lock(&mutex_log);
            log_error(logger, "Query %d: TAG falló para %s:%s -> %s:%s - Código desconocido: %d",
                      query_id,
                      nombre_archivo_origen, nombre_etiqueta_origen,
                      nombre_archivo_destino, nombre_etiqueta_destino,
                      respuesta->cod_op);
            pthread_mutex_unlock(&mutex_log);
            break;
    }

    if (respuesta->payload)
        payload_destroy(respuesta->payload);
    liberar_paquete(respuesta);

    free(nombre_archivo_origen);
    free(nombre_etiqueta_origen);
    free(nombre_archivo_destino);
    free(nombre_etiqueta_destino);
}

void instruccion_commit(char* file_tag, uint32_t query_id) {
    char* nombre_archivo;
    char* nombre_etiqueta;

    if (!parsear_file_tag(file_tag, &nombre_archivo, &nombre_etiqueta)) {
        pthread_mutex_lock(&mutex_log);
        log_error(logger, "Query %d: Formato inválido de ARCHIVO:ETIQUETA en COMMIT: %s", query_id, file_tag);
        pthread_mutex_unlock(&mutex_log);
        return;
    }

    instruccion_flush(file_tag, query_id); // hace un flush antes del commit


    uint32_t tam_payload =
          sizeof(uint32_t)
        + sizeof(uint32_t) + strlen(nombre_archivo) + 1
        + sizeof(uint32_t) + strlen(nombre_etiqueta) + 1;

    payload_t* payload = payload_create(tam_payload);

    payload_add(payload, &query_id, sizeof(uint32_t));         
    payload_add_string(payload, nombre_archivo);                
    payload_add_string(payload, nombre_etiqueta);               

    paquete_t* paquete = crear_paquete(OP_COMMIT, payload);
    if (enviar_paquete(conexion_storage, paquete) != OK) {
        pthread_mutex_lock(&mutex_log);
        log_error(logger, "Query %d: Error al enviar COMMIT a Storage", query_id);
        pthread_mutex_unlock(&mutex_log);
        payload_destroy(payload);
        liberar_paquete(paquete);
        free(nombre_archivo);
        free(nombre_etiqueta);
        return;
    }

    payload_destroy(payload);
    liberar_paquete(paquete);

    paquete_t* respuesta = recibir_paquete(conexion_storage);
    if (respuesta == NULL) {
        pthread_mutex_lock(&mutex_log);
        log_error(logger, "Query %d: No se recibió respuesta de Storage para COMMIT", query_id);
        pthread_mutex_unlock(&mutex_log);
        free(nombre_archivo);
        free(nombre_etiqueta);
        return;
    }

    // Manejo explícito de códigos esperados
    if (respuesta->cod_op == OP_EXITOSA) {

        pthread_mutex_lock(&mutex_resultado_operacion);
        resultado_operacion = OP_EXITOSA;
        pthread_mutex_unlock(&mutex_resultado_operacion);

    } else if (respuesta->cod_op == FILE_TAG_INEXISTENTE) {

        enviar_error_query_master(query_id, FILE_TAG_INEXISTENTE);
        pthread_mutex_lock(&mutex_resultado_operacion);
        resultado_operacion = FILE_TAG_INEXISTENTE;
        pthread_mutex_unlock(&mutex_resultado_operacion);

    } else if (respuesta->cod_op == ESCRITURA_NO_PERMITIDA) {

        enviar_error_query_master(query_id, ESCRITURA_NO_PERMITIDA);
        pthread_mutex_lock(&mutex_resultado_operacion);
        resultado_operacion = ESCRITURA_NO_PERMITIDA;
        pthread_mutex_unlock(&mutex_resultado_operacion);

    } else {
        pthread_mutex_lock(&mutex_log);
        log_error(logger, "Query %d: COMMIT falló para %s:%s - Código desconocido: %d", query_id, nombre_archivo, nombre_etiqueta, respuesta->cod_op);
        pthread_mutex_unlock(&mutex_log);
    }

    // limpiar y salir
    if (respuesta->payload) payload_destroy(respuesta->payload);
    liberar_paquete(respuesta);
    free(nombre_archivo);
    free(nombre_etiqueta);
}

void instruccion_flush(char* file_tag, uint32_t query_id) {
    char* nombre_archivo;
    char* nombre_etiqueta;

    if (!parsear_file_tag(file_tag, &nombre_archivo, &nombre_etiqueta)) {
        pthread_mutex_lock(&mutex_log);
        log_error(logger, "Query %d: Formato inválido de ARCHIVO:ETIQUETA en FLUSH: %s", query_id, file_tag);
        pthread_mutex_unlock(&mutex_log);
        return;
    }

    t_tabla_paginas* tabla = traer_tabla(memoria, nombre_archivo, nombre_etiqueta);
    if (tabla == NULL) {
        pthread_mutex_lock(&mutex_log);
        log_error(logger, "Query %d: No se encontró tabla de páginas para %s:%s en FLUSH", query_id, nombre_archivo, nombre_etiqueta);
        pthread_mutex_unlock(&mutex_log);
        free(nombre_archivo);
        free(nombre_etiqueta);
        return;
    }

    // itera sobre todas las paginas de la tabla
    for (int i = 0; i < list_size(tabla->paginas); i++) {
        t_pagina* pagina = list_get(tabla->paginas, i);
        if (pagina->presente && pagina->modificado) {
            uint32_t tam_pagina = memoria->tam_pagina;
            void* direccion_fisica = memoria->memoria + (pagina->marco * tam_pagina);
            void* valor_escrito = malloc(tam_pagina);
            memcpy(valor_escrito, direccion_fisica, tam_pagina);

            

            flush_pagina_a_storage(nombre_archivo, nombre_etiqueta, pagina->numero_pagina, pagina->marco, query_id);

            pagina->modificado = false;

            free(valor_escrito);
        }
    }

    free(nombre_archivo);
    free(nombre_etiqueta);
}

void instruccion_delete(char* file_tag, uint32_t query_id) {
    char* nombre_archivo;
    char* nombre_etiqueta;

    if (!parsear_file_tag(file_tag, &nombre_archivo, &nombre_etiqueta)) {
        pthread_mutex_lock(&mutex_log);
        log_error(logger, "Query %d: Formato inválido de ARCHIVO:ETIQUETA en DELETE: %s", query_id, file_tag);
        pthread_mutex_unlock(&mutex_log);
        return;
    }
    uint32_t tam_payload =
          sizeof(uint32_t)
        + sizeof(uint32_t) + strlen(nombre_archivo) + 1
        + sizeof(uint32_t) + strlen(nombre_etiqueta) + 1;

    payload_t* payload = payload_create(tam_payload);

    payload_add(payload, &query_id, sizeof(uint32_t));          
    payload_add_string(payload, nombre_archivo);                
    payload_add_string(payload, nombre_etiqueta);               

    paquete_t* paquete = crear_paquete(OP_DELETE, payload);
    if (enviar_paquete(conexion_storage, paquete) != OK) {
        pthread_mutex_lock(&mutex_log);
        log_error(logger, "Query %d: Error al enviar DELETE a Storage", query_id);
        pthread_mutex_unlock(&mutex_log);
        payload_destroy(payload);
        liberar_paquete(paquete);
        free(nombre_archivo);
        free(nombre_etiqueta);
        return;
    }

    payload_destroy(payload);
    liberar_paquete(paquete);

    paquete_t* respuesta = recibir_paquete(conexion_storage);
    if (respuesta == NULL) {
        pthread_mutex_lock(&mutex_log);
        log_error(logger, "Query %d: No se recibió respuesta de Storage para DELETE", query_id);
        pthread_mutex_unlock(&mutex_log);
        free(nombre_archivo);
        free(nombre_etiqueta);
        return;
    }

    if (respuesta->cod_op == OP_EXITOSA) {

        pthread_mutex_lock(&mutex_resultado_operacion);
        resultado_operacion = OP_EXITOSA;
        pthread_mutex_unlock(&mutex_resultado_operacion);

    } else if (respuesta->cod_op == FILE_TAG_INEXISTENTE) {

        enviar_error_query_master(query_id, FILE_TAG_INEXISTENTE);
        pthread_mutex_lock(&mutex_resultado_operacion);
        resultado_operacion = FILE_TAG_INEXISTENTE;
        pthread_mutex_unlock(&mutex_resultado_operacion);

    } else {
        pthread_mutex_lock(&mutex_log);
        log_error(logger, "Query %d: DELETE falló para %s:%s - Código: %d", query_id, nombre_archivo, nombre_etiqueta, respuesta->cod_op);
        pthread_mutex_unlock(&mutex_log);
    }

    if (respuesta->payload) payload_destroy(respuesta->payload);
    liberar_paquete(respuesta);
    free(nombre_archivo);
    free(nombre_etiqueta);
}


void instruccion_end(uint32_t query_id) {
    payload_t* payload = payload_create(sizeof(uint32_t));
    payload_add(payload, &query_id, sizeof(uint32_t));

    paquete_t* paquete = crear_paquete(OP_END, payload);
    if (enviar_paquete(conexion_master, paquete) != OK) {
        pthread_mutex_lock(&mutex_log);
        log_error(logger, "Query %d: Error al enviar END a Master", query_id);
        pthread_mutex_unlock(&mutex_log);
        payload_destroy(payload);
        liberar_paquete(paquete);
        return;
    }

    payload_destroy(payload);
    liberar_paquete(paquete);

}

bool parsear_file_tag(char* file_tag, char** nombre_archivo, char** nombre_etiqueta) {
    char* separador = strchr(file_tag, ':');
    if(separador == NULL) {
        return false;
    }
    
    int longitud_archivo = separador - file_tag;
    *nombre_archivo = malloc(longitud_archivo + 1);
    strncpy(*nombre_archivo, file_tag, longitud_archivo);
    (*nombre_archivo)[longitud_archivo] = '\0';
    
    *nombre_etiqueta = strdup(separador + 1);
    
    return true;
}

void flush_files_modificados(uint32_t query_id) {
    // Itera sobre todas las tablas de páginas en memoria
    pthread_mutex_lock(&mutex_memoria);
    t_list* tablas = dictionary_elements(memoria->tabla_paginas);
    for (int i = 0; i < list_size(tablas); i++) {
        t_tabla_paginas* tabla = list_get(tablas, i);

        // Verifica si hay páginas modificadas en esta tabla
        bool tiene_modificadas = false;
        for (int j = 0; j < list_size(tabla->paginas); j++) {
            t_pagina* pagina = list_get(tabla->paginas, j);
            if (pagina->presente && pagina->modificado) {
                tiene_modificadas = true;
                break;
            }
        }

        if (tiene_modificadas) {
            // Construye file_tag para llamar a instruccion_flush
            char file_tag[256];
            snprintf(file_tag, sizeof(file_tag), "%s:%s", tabla->file, tabla->tag);
            instruccion_flush(file_tag, query_id);
        }
    }
    list_destroy(tablas);
    pthread_mutex_unlock(&mutex_memoria);
}

void enviar_error_query_master(uint32_t query_id, uint32_t codigo_error) {
    uint32_t tam_payload = sizeof(uint32_t) + sizeof(uint32_t);

    payload_t* payload = payload_create(tam_payload);
    payload_add(payload, &query_id, sizeof(uint32_t));
    payload_add(payload, &codigo_error, sizeof(uint32_t));

    paquete_t* paquete = crear_paquete(FINALIZAR_QUERY_CON_ERROR, payload);
    if (enviar_paquete(conexion_master, paquete) != OK) {
        pthread_mutex_lock(&mutex_log);
        log_error(logger, "Query %d: Error al enviar el codigo de error a Master", query_id);
        pthread_mutex_unlock(&mutex_log);
    }

    payload_destroy(payload);
    liberar_paquete(paquete);
}

