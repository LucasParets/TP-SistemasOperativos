#include "main.h"

//config
t_config* config;
t_master_config master_config;

//loggers
t_log* logger;
t_log* logger_de_comunicacion;

//sockets
int master_server, conexion_con_qc;

// contadores de ids
int contador_ids_query = 0;
int contador_ids_qc = 0;

//listas de modulos y querys
t_list* qc_conectados;
t_list* workers_conectados;
t_list* lista_queries; // para tener registro completo 

//colas de planificacion
t_queue* cola_ready;
t_queue* cola_exec;

//hilos
pthread_t hilo_conexion;
pthread_t hilo_peticiones_qc;
pthread_t hilo_peticiones_worker;
pthread_t hilo_planificador_fifo;
pthread_t hilo_planificador_prioridades;
pthread_t hilo_aging;

//semaforos
pthread_mutex_t mutex_contador_id = PTHREAD_MUTEX_INITIALIZER;
pthread_mutex_t mutex_queries = PTHREAD_MUTEX_INITIALIZER;
pthread_mutex_t mutex_ready = PTHREAD_MUTEX_INITIALIZER;
pthread_mutex_t mutex_exec = PTHREAD_MUTEX_INITIALIZER;
pthread_mutex_t mutex_workers = PTHREAD_MUTEX_INITIALIZER;
pthread_mutex_t mutex_qc = PTHREAD_MUTEX_INITIALIZER;
pthread_mutex_t mutex_logs = PTHREAD_MUTEX_INITIALIZER;
sem_t sem_queries_ready;
sem_t sem_workers_libres;
sem_t sem_eventos_replanificar;
sem_t se_puede_asignar;

int main(int argc, char* argv[]) {
    
    printf("=== INICIANDO MASTER ===\n");
    
    //iniciamos la config
    load_master_config(argv[1]);

    //iniciamos los loggers
    logger = iniciar_logger("master.log", "MASTER", 1, LOG_LEVEL_INFO);
    logger_de_comunicacion = iniciar_logger("master_debug.log", "MASTER", 1, LOG_LEVEL_DEBUG);

    //inicializamos las listas de conexiones 
    qc_conectados = list_create();
    workers_conectados = list_create();
    lista_queries = list_create();

    //inicializamos colas de planificacion
    cola_ready = queue_create();
    cola_exec = queue_create();

    // inicializamos semáforos contadores
    sem_init(&sem_queries_ready, 0, 0);
    sem_init(&sem_workers_libres, 0, 0);
    sem_init(&sem_eventos_replanificar, 0, 0); //mide cambios en cola de ready, workers libres y cambios de prioridad para el planificador prioridades
    sem_init(&se_puede_asignar, 0, 0);
    //MASTER inicia un servidor que escucha por conexiones de Query Control y WORKER
    master_server = escuchar_conexiones_de("QUERY CONTROL y WORKER", master_config.puerto_escucha, logger_de_comunicacion);

    // MASTER acepta conexiones y las distribuye según el handshake
    pthread_create(&hilo_conexion, NULL, aceptar_conexiones, NULL);

    // MASTER inicia hilos aparte para la planificacion 
    if (strcmp(master_config.algoritmo_planificacion, "FIFO") == 0) {
        //Fifo
        pthread_create(&hilo_planificador_fifo, NULL, planificador_fifo, NULL);
        pthread_detach(hilo_planificador_fifo);
    } else if (strcmp(master_config.algoritmo_planificacion, "PRIORIDADES") == 0) {
        //Prioridades
        pthread_create(&hilo_planificador_prioridades, NULL, planificador_prioridades, NULL);
        pthread_detach(hilo_planificador_prioridades);
        //Aging
        ///pthread_create(&hilo_aging, NULL, hilo_aging_funcion, NULL);
        //pthread_detach(hilo_aging);
    } else {
        pthread_mutex_lock(&mutex_logs);
        log_error(logger_de_comunicacion, "Algoritmo de planificacion incorrecto, cerrando Master");
        pthread_mutex_unlock(&mutex_logs);
        pthread_join(hilo_conexion, NULL);
        liberar_master();
    }

    
    //Terminamos el master
    pthread_join(hilo_conexion, NULL);
    //liberar_master(); //liberamos todos los recursos del master
    return 0;
}

// ********************* SERVER MULTIHILO DE MASTER *********************

t_qc* crear_referencia_a_qc(int socket){
    t_qc* nuevo_qc = malloc(sizeof(t_qc)); 
    pthread_mutex_lock(&mutex_contador_id);
    nuevo_qc->id = contador_ids_qc;
    contador_ids_qc++;
    pthread_mutex_unlock(&mutex_contador_id);
    nuevo_qc->conexion_qc = socket;
    return nuevo_qc;
}

t_worker* crear_referencia_a_worker(int id, int socket){
    t_worker* nuevo_worker = malloc(sizeof(t_worker));
    nuevo_worker->id = id;
    nuevo_worker->conexion_worker = socket;
    nuevo_worker->estado = LIBRE;
    nuevo_worker->query_actual = -1;
    return nuevo_worker;
}

void* atender_qc(void* s){
    int conexion_con_qc = *(int*)s;
    free(s);
    t_qc* qc_actual = crear_referencia_a_qc(conexion_con_qc);
    pthread_mutex_lock(&mutex_qc);
    list_add(qc_conectados, qc_actual);
    pthread_mutex_unlock(&mutex_qc);
    while(1){
        paquete_t* pack = recibir_paquete(conexion_con_qc);

        if (pack == NULL)
        {
            // Se desconectó un QC
            if(qc_actual != NULL){

            pthread_mutex_lock(&mutex_workers);
            pthread_mutex_lock(&mutex_logs);    
            log_info(logger, "\n## Se desconecta un Query Control. Se finaliza la Query %d con prioridad %d. Nivel multiprocesamiento %d", 
                            qc_actual->id,
                            qc_actual->query_asociada->prioridad,
                            list_size(workers_conectados));
            pthread_mutex_unlock(&mutex_logs);
            pthread_mutex_unlock(&mutex_workers);
            pthread_mutex_lock(&mutex_queries);
            if(qc_actual->query_asociada != NULL){
                t_query* q = qc_actual->query_asociada; // chequea query asociada a ese qc
                pthread_mutex_unlock(&mutex_queries);
                cancelar_query(q);
            } else {
                pthread_mutex_unlock(&mutex_queries);
            }
            close(conexion_con_qc);
            pthread_mutex_lock(&mutex_qc);
            list_remove_element(qc_conectados, qc_actual);
            pthread_mutex_unlock(&mutex_qc);

            free(qc_actual);
        } else{
            close(conexion_con_qc);
        }    
            pthread_exit(NULL);
        }

        switch (pack->cod_op)
        {
            case MANDAR_QUERY:
            {
                char* path_query = payload_read_string(pack->payload);
                int prioridad;
                payload_read(pack->payload, &prioridad, sizeof(int));

                
                //creamos la query
                t_query* nueva_query = malloc(sizeof(t_query));
                pthread_mutex_lock(&mutex_contador_id);
                nueva_query->id = contador_ids_query++;
                pthread_mutex_unlock(&mutex_contador_id);
                nueva_query->path = strdup(path_query);
                nueva_query->prioridad = prioridad;
                nueva_query->estado = READY;
                nueva_query->worker_asignado = -1;
                nueva_query->program_counter = 0;
                //nueva_query->tiempo_en_ready = time(NULL);
                nueva_query->fue_desalojada = false;
                nueva_query->vez_en_ready = 1;
                
                //la asociamos al qc que se conecto
                qc_actual->query_asociada = nueva_query;

                pthread_mutex_lock(&mutex_queries);
                list_add(lista_queries, qc_actual->query_asociada);
                pthread_mutex_unlock(&mutex_queries);


                pthread_mutex_lock(&mutex_logs);
                log_info(logger,"\n## Se conecta un Query Control para ejecutar la Query %s con prioridad %d - Id asignado:%d - Nivel multiprocesamiento: %d",
                        nueva_query->path,
                        nueva_query->prioridad,
                        nueva_query->id,
                        list_size(workers_conectados));
                pthread_mutex_unlock(&mutex_logs);

                pthread_mutex_lock(&mutex_ready);
                queue_push(cola_ready, nueva_query);
                pthread_mutex_unlock(&mutex_ready); //ver posible uso de monitores  

                if(strcmp(master_config.algoritmo_planificacion, "PRIORIDADES")== 0){
                    // SÓLO SI EL ALGO DE PLANIF ES POR PRORIDADES: se crea hilo de aging para la query recien  llegada a ready
                    t_contexto_aging* contexto = malloc(sizeof(t_contexto_aging));
                    contexto->query = nueva_query;
                    contexto->vez_de_entrada = nueva_query->vez_en_ready;

                    pthread_create(&(nueva_query->hilo_aging), NULL, hilo_aging_individual, contexto);
                    pthread_detach(nueva_query->hilo_aging);
                } 

                sem_post(&sem_queries_ready);
                sem_post(&sem_eventos_replanificar);

                // Liberamos path_query despues de usarlo
                free(path_query);

                break;         
            }
        }
        
        
        payload_destroy(pack->payload);
        liberar_paquete(pack);
    }
}

void* atender_worker(void* s){
    int conexion_con_worker = *(int*)s;
    free(s);
    t_worker* worker_actual = NULL;

    while(1){
        
        paquete_t* pack = recibir_paquete(conexion_con_worker);

        if (pack == NULL)
        {
            if(worker_actual != NULL){
            pthread_mutex_lock(&mutex_workers);    
            pthread_mutex_lock(&mutex_logs);
            log_info(logger, "\n## Se desconecta el Worker %d - Se finaliza la Query %d - Cantidad total de Workers %d",
                            worker_actual->id,
                            worker_actual->query_actual,
                            list_size(workers_conectados) - 1); //a chequear esto
            pthread_mutex_unlock(&mutex_logs);
            pthread_mutex_unlock(&mutex_workers);

            if(worker_actual->query_actual != -1) {
                int id_query = worker_actual->query_actual;

                // Busco la Query asociada a partir de ese id
                pthread_mutex_lock(&mutex_workers); 
                t_query* q = buscar_query_por_id(id_query);
                pthread_mutex_unlock(&mutex_workers); 
                if(q != NULL){
                    q->estado = EXIT;
                    q->worker_asignado = -1;

                    notificar_desconexion_a_qc(q);
                }
             }  

            // libero el  Worker
            pthread_mutex_lock(&mutex_workers);
            list_remove_element(workers_conectados, worker_actual);
            pthread_mutex_unlock(&mutex_workers);

            close(worker_actual->conexion_worker);
            free(worker_actual);

            } else {
                close(conexion_con_worker);
            }
            pthread_exit(NULL);       
            break;
        }

        switch (pack->cod_op)
        {
            case MANDAR_ID:
            {
                int id;
                payload_read(pack->payload, &id, sizeof(id));
                worker_actual = crear_referencia_a_worker(id, conexion_con_worker);
                pthread_mutex_lock(&mutex_workers);
                list_add(workers_conectados, worker_actual);
                pthread_mutex_unlock(&mutex_workers);
                pthread_mutex_lock(&mutex_logs);
                log_info(logger, "\n## Se conecta el Worker %d - Cantidad total de Workers: %d",
                         worker_actual->id, 
                         list_size(workers_conectados));
                pthread_mutex_unlock(&mutex_logs);

                sem_post(&sem_workers_libres);
                sem_post(&sem_eventos_replanificar);

                break;
            }
            case DESALOJO_WORKER:
            {
                int query_id;
                uint32_t pc;
                payload_read(pack->payload, &query_id, sizeof(uint32_t));
                payload_read(pack->payload, &pc, sizeof(uint32_t));
                
                // Buscamos la query desalojada o cancelada y actualizamos su PC
                pthread_mutex_lock(&mutex_queries);
                for(int i = 0; i < list_size(lista_queries); i++) {
                    t_query* q = list_get(lista_queries, i);
                    if(q->id == query_id && q->estado == EXEC) {

                        // Cambiamos el estado de la query desalojada
                        q->estado = READY;
                        q->worker_asignado = -1;
                        //query_a_desalojar->tiempo_en_ready = time(NULL);
                        pthread_mutex_lock(&mutex_exec);
                        list_remove_element(cola_exec->elements, q);
                        pthread_mutex_unlock(&mutex_exec);
                        pthread_mutex_lock(&mutex_ready);
                        queue_push(cola_ready, q);
                        pthread_mutex_unlock(&mutex_ready);
                        // Esto no deberia entrar a cola_exec???!!!!

                        q->program_counter = pc;
                        q->fue_desalojada = true;
                       // q->tiempo_en_ready = time(NULL); // Reiniciar tiempo para aging
                        q->vez_en_ready++; // aumenta para que el hilo no se confunda y no le cambie la prioridad a una q que fue a exec y fue desalojada

                    t_contexto_aging* contexto = malloc(sizeof(t_contexto_aging));
                    contexto->query = q;
                    contexto->vez_de_entrada = q->vez_en_ready;
                    
                    sem_post(&se_puede_asignar);
                    pthread_create(&(q->hilo_aging), NULL, hilo_aging_individual, contexto);
                    pthread_detach(q->hilo_aging);
                 
                    break;

                    }
                }
                pthread_mutex_unlock(&mutex_queries);
                     
                break;
            }
            case RESULTADO_LECTURA:
            {   
                int id_query;
                uint32_t tamanio;

                payload_read(pack->payload, &id_query, sizeof(uint32_t));
                char* file = payload_read_string(pack->payload);
                char* tag  = payload_read_string(pack->payload);
                payload_read(pack->payload, &tamanio, sizeof(uint32_t));
                void* contenido = malloc(tamanio);
                payload_read(pack->payload, contenido, tamanio);
                
                pthread_mutex_lock(&mutex_queries);

                t_query* q_asociada = buscar_query_por_id(id_query);

                t_qc* qc_destino = NULL;

                if (q_asociada != NULL) {
                    pthread_mutex_lock(&mutex_qc);                 
                    qc_destino = buscar_qc_asociado(q_asociada);
                    pthread_mutex_unlock(&mutex_qc);

                }
                pthread_mutex_unlock(&mutex_queries);

                // se le envía el res recibido por el worker al qc asociado
                if (qc_destino != NULL){ 

                    reenviar_lectura_a_qc(qc_destino, id_query, file, tag, contenido, tamanio);
                    pthread_mutex_lock(&mutex_logs);
                    log_info(logger, "\n## Se envía un mensaje de lectura de la Query %d en el Worker %d al Query Control %d",
                                    id_query,
                                    worker_actual->id,
                                    qc_destino->id); // la parte del qc no está en el log obligatorio pero supongo que es útil agregarlo
                    pthread_mutex_unlock(&mutex_logs);    
                }

                free(file);
                free(tag);
                free(contenido);
                break;
            }
            case FINALIZAR_QUERY_CON_ERROR:
            {
                uint32_t id_query;
                payload_read(pack->payload, &id_query, sizeof(uint32_t));
                uint32_t codigo_error;
                payload_read(pack->payload, &codigo_error, sizeof(uint32_t));
                pthread_mutex_lock(&mutex_queries);
                t_query* q_asociada = buscar_query_por_id(id_query);
                t_qc* qc_destino = NULL;
                if (q_asociada != NULL) {
                    pthread_mutex_lock(&mutex_qc);
                    qc_destino = buscar_qc_asociado(q_asociada);
                    pthread_mutex_unlock(&mutex_qc);

                }
                pthread_mutex_unlock(&mutex_queries);

                // se le envía el error recibido por el worker al qc asociado
                if (qc_destino != NULL){ 

                    reenviar_error_a_qc(qc_destino, codigo_error);

                }

                pthread_mutex_lock(&mutex_queries);
                if(q_asociada != NULL){
                    q_asociada-> estado = EXIT;
                    q_asociada->worker_asignado = -1;
                }
                
                pthread_mutex_lock(&mutex_exec);
                list_remove_element(cola_exec->elements,q_asociada);
                /*for(int i = 0; i < list_size(cola_exec->elements); i++){

                    t_query* q_a_eliminar_exec = list_get(cola_exec->elements, i);

                    if(q_a_eliminar_exec->id == q_asociada->id){
                        list_remove(cola_exec->elements, i);
                    }
                } */
                pthread_mutex_unlock(&mutex_exec);
                pthread_mutex_unlock(&mutex_queries);

                pthread_mutex_lock(&mutex_workers);
                worker_actual->estado = LIBRE;
                worker_actual->query_actual = -1;
                pthread_mutex_unlock(&mutex_workers);

                sem_post(&sem_workers_libres);
                sem_post(&sem_eventos_replanificar);

                break;
            }
            case OP_END:
            {
                int id_query;
                payload_read(pack->payload, &id_query, sizeof(uint32_t));

                pthread_mutex_lock(&mutex_queries);
                t_query* q_asociada = buscar_query_por_id(id_query);
                t_qc* qc_destino = NULL;
                if (q_asociada != NULL) {
                    pthread_mutex_lock(&mutex_qc);
                    qc_destino = buscar_qc_asociado(q_asociada);
                    pthread_mutex_unlock(&mutex_qc);
                }
                pthread_mutex_unlock(&mutex_queries);
                
                if (qc_destino != NULL){ 

                    enviar_finalizacion_a_qc(qc_destino);
                }

                pthread_mutex_lock(&mutex_queries);
                if(q_asociada != NULL){
                    q_asociada-> estado = EXIT;
                    q_asociada->worker_asignado = -1;
                }
                
                pthread_mutex_lock(&mutex_exec);
                list_remove_element(cola_exec->elements,q_asociada);
                /*for(int i = 0; i < list_size(cola_exec->elements); i++){

                    t_query* q_a_eliminar_exec = list_get(cola_exec->elements, i);

                    if(q_a_eliminar_exec->id == q_asociada->id){
                        list_remove(cola_exec->elements, i);
                    }
                }*/
                pthread_mutex_unlock(&mutex_exec);
                pthread_mutex_unlock(&mutex_queries);

                pthread_mutex_lock(&mutex_workers);
                worker_actual->estado = LIBRE;
                worker_actual->query_actual = -1;
                pthread_mutex_unlock(&mutex_workers);

                sem_post(&sem_workers_libres);
                sem_post(&sem_eventos_replanificar);

                pthread_mutex_lock(&mutex_logs);
                log_info(logger, "\n## Se terminó la Query %d en el Worker %d", id_query, worker_actual->id);
                pthread_mutex_unlock(&mutex_logs);

                break;
            }
        }
        
        
        payload_destroy(pack->payload);
        liberar_paquete(pack);
    }
}

void* aceptar_conexiones(){
    while(1){
        // MASTER espera que QUERY CONTROL o WORKER se conecte
        int* conexion_con_modulo = malloc(sizeof(int));
        *conexion_con_modulo = accept(master_server, NULL, NULL);
        
        if(conexion_con_modulo < 0) {
            pthread_mutex_lock(&mutex_logs);
            log_error(logger_de_comunicacion, "MODULO QUERY CONTROL O WORKER NO PUDO CONECTARSE CON MASTER!");
            pthread_mutex_unlock(&mutex_logs);
            liberar_master();
            exit(EXIT_FAILURE);
        }

        //mandamos el handshake para verificar
        conexion_t handshake = handshake_con_cliente(*conexion_con_modulo);

        if (handshake != QC_CON_MASTER && handshake != WORKER_CON_MASTER) {
            perror("Error en handshake!");
            close(*conexion_con_modulo);
            close(master_server);
            return NULL;
        }

        switch (handshake)
        {
            case QC_CON_MASTER:
            pthread_mutex_lock(&mutex_logs);
            log_debug(logger_de_comunicacion, "MODULO QUERY CONTROL CONECTO CON MASTER EXITOSAMENTE!");
            pthread_mutex_unlock(&mutex_logs);
                pthread_create(&hilo_peticiones_qc, NULL, atender_qc, conexion_con_modulo);
                pthread_detach(hilo_peticiones_qc);
                break;
            
            case WORKER_CON_MASTER:
                pthread_mutex_lock(&mutex_logs);
                log_debug(logger_de_comunicacion, "MODULO WORKER CONECTO CON MASTER EXITOSAMENTE!");
                pthread_mutex_unlock(&mutex_logs);
                pthread_create(&hilo_peticiones_worker, NULL, atender_worker, conexion_con_modulo);
                pthread_detach(hilo_peticiones_worker);
                break;
            
            default:
                pthread_mutex_lock(&mutex_logs);
                log_error(logger_de_comunicacion, "Handshake incorrecto! Tipo recibido: %d", handshake);
                pthread_mutex_unlock(&mutex_logs);

                pthread_mutex_lock(&mutex_logs);
                log_error(logger_de_comunicacion, "Esperado: QC_CON_MASTER (%d) o WORKER_CON_MASTER (%d)", QC_CON_MASTER, WORKER_CON_MASTER);
                pthread_mutex_unlock(&mutex_logs);

                close(*conexion_con_modulo);
                break;
            }
        }
}

// ********************* FIN SERVER MULTIHILO DE MASTER *********************


void load_master_config(char* path) {

    config = iniciar_config(path);

    if(config == NULL) {
        fprintf(stderr, "Config invalido!\n");
        exit(EXIT_FAILURE);
    }

    master_config.puerto_escucha = config_get_string_value(config,"PUERTO_ESCUCHA");
    master_config.algoritmo_planificacion = config_get_string_value(config,"ALGORITMO_PLANIFICACION");
    master_config.tiempo_aging = config_get_int_value(config,"TIEMPO_AGING");
    master_config.log_level = config_get_string_value(config,"LOG_LEVEL");
}

// funcion para destruir queries
void destruir_query(void* query_ptr){
    if(query_ptr != NULL){
        t_query* q = (t_query*)query_ptr;
        if(q->path != NULL){
            free(q->path);
        }
        free(q);
    }
}

void liberar_master() {


    // libero todas las listas
    list_destroy_and_destroy_elements(workers_conectados, free);
    list_destroy_and_destroy_elements(qc_conectados, free);
    list_destroy_and_destroy_elements(lista_queries, destruir_query);

    // libero colas
    queue_destroy(cola_ready);
    queue_destroy(cola_exec);

    // destruyo los mutex y contadores
    pthread_mutex_destroy(&mutex_ready);
    pthread_mutex_destroy(&mutex_exec);
    pthread_mutex_destroy(&mutex_workers);
    pthread_mutex_destroy(&mutex_queries);
    sem_destroy(&sem_queries_ready);
    sem_destroy(&sem_workers_libres);
    sem_destroy(&sem_eventos_replanificar);

    close(master_server);
    close(conexion_con_qc);
    log_destroy(logger);
    log_destroy(logger_de_comunicacion);
    config_destroy(config);
}

// ********************* PLANIFICADOR FIFO *********************

void* planificador_fifo(){
    while(1){
        
        sem_wait(&sem_queries_ready);
        sem_wait(&sem_workers_libres); //usamos estos dos semáforos para que el planificador corra SÓLO si hay una nueva query en ready o se libera un worker

        pthread_mutex_lock(&mutex_ready);
        bool hay_queries_en_ready = !queue_is_empty(cola_ready);
        t_query* q = NULL;
        
        if(hay_queries_en_ready){
            q = queue_pop(cola_ready); //ya de por sí funciona con FIFO
        }
        pthread_mutex_unlock(&mutex_ready);

        pthread_mutex_lock(&mutex_workers);
        t_worker* worker_libre = buscar_worker_libre();

        // primero corrobora que hayan queries esperando y que alguno de los workers esté libre

        if(q != NULL && worker_libre != NULL){
            
            asignar_a_worker(q, worker_libre); // configuraciones correspondientes tanto en la query como en el worker (ej cambios de estados)
            pthread_mutex_unlock(&mutex_workers);
        } else { // manejo errores x las dudas; casos raros, algún fallo de desconexión entre medio u otra cosa 
            pthread_mutex_unlock(&mutex_workers);
            if(q != NULL){
                pthread_mutex_lock(&mutex_ready);
                queue_push(cola_ready, q);
                pthread_mutex_unlock(&mutex_ready);
                sem_post(&sem_queries_ready);
            }
            pthread_mutex_lock(&mutex_workers);
            if(worker_libre != NULL){
                sem_post(&sem_workers_libres);
            } // en ambos casos libero semáforos (y devuelvo la query a la cola en caso de no poder asignarla)
            pthread_mutex_unlock(&mutex_workers);
        }

    }
}

t_worker* buscar_worker_libre(){
    for(int i = 0; i < list_size(workers_conectados); i++){
        t_worker* w = list_get(workers_conectados, i);
        if(w->estado == LIBRE) return w;
    }
    return NULL;
}

void asignar_a_worker(t_query* q, t_worker* worker_libre){

    //Configuro la query
    pthread_mutex_lock(&mutex_queries);
    q->estado = EXEC;
    q->worker_asignado = worker_libre->id;
    pthread_mutex_unlock(&mutex_queries);


    //Configuro el worker
    worker_libre->estado = OCUPADO;
    worker_libre->query_actual = q->id;

    pthread_mutex_lock(&mutex_exec);
    queue_push(cola_exec, q);
    pthread_mutex_unlock(&mutex_exec); //acá también ver tema de monitores

    enviar_query_a_worker(worker_libre, q); 

}

void enviar_query_a_worker(t_worker* worker, t_query* query){

    uint32_t size = sizeof(int) + sizeof(int) + strlen(query->path) + 1 + sizeof(uint32_t);
    payload_t* payload = payload_create(size);

    payload_add(payload, &(query->id), sizeof(int));
    payload_add_string(payload, query->path);
    //payload_add(payload, &(query->prioridad), sizeof(int)); NO HACE FALTA PARA EL WORKER, 
    payload_add(payload, &(query->program_counter), sizeof(uint32_t));

    paquete_t* paquete = crear_paquete(MANDAR_QUERY, payload);

    enviar_paquete(worker->conexion_worker, paquete);

    pthread_mutex_lock(&mutex_logs);
    log_info(logger, "\n## Se envía la Query <%d> (<%d>) al Worker <%d>", 
                         query->id, query->prioridad, worker->id);
    pthread_mutex_unlock(&mutex_logs);
    
    payload_destroy(paquete->payload);
    liberar_paquete(paquete);
}

// ********************* FIN PLANIFICADOR FIFO *********************

// ********************* PLANIFICADOR PRIORIDADES *********************

void* planificador_prioridades(){
    while(1){
        // Posible opcion, sem_wait(&desalojo); 
        sem_wait(&sem_eventos_replanificar);
        pthread_mutex_lock(&mutex_ready);
        bool hay_queries_en_ready = !queue_is_empty(cola_ready);
        pthread_mutex_unlock(&mutex_ready);

        pthread_mutex_lock(&mutex_workers);
        t_worker* worker_libre = buscar_worker_libre();
        pthread_mutex_unlock(&mutex_workers);

        if(hay_queries_en_ready){
            // Ordenamos la cola por prioridad
            pthread_mutex_lock(&mutex_ready);
            list_sort(cola_ready->elements, comparar_prioridades);
            t_query* query_prioritaria = queue_pop(cola_ready);
            pthread_mutex_unlock(&mutex_ready);

            pthread_mutex_lock(&mutex_workers);
            if(worker_libre != NULL){
                asignar_a_worker(query_prioritaria, worker_libre);
                pthread_mutex_unlock(&mutex_workers);
            } else { 
                // No hay workers libres, verificamos si se puede desalojar
                t_worker* worker_con_menor_prioridad = buscar_worker_con_menor_prioridad(query_prioritaria->prioridad);
                pthread_mutex_unlock(&mutex_workers);

                pthread_mutex_lock(&mutex_workers);
                if(worker_con_menor_prioridad != NULL){
                    // Se puede desalojar
                    desalojar_query_de_worker(worker_con_menor_prioridad, query_prioritaria);
                    pthread_mutex_unlock(&mutex_workers); 
                } else {
                    pthread_mutex_unlock(&mutex_workers); 
                    pthread_mutex_lock(&mutex_ready);
                    // No se puede desalojar, volvemos a ponerla en cola
                    queue_push(cola_ready, query_prioritaria);
                    pthread_mutex_unlock(&mutex_ready);
                }
            }
        } 
      
    }
}


// t_worker* buscar_worker_con_menor_prioridad(int prioridad_nueva){
//     t_worker* worker_con_menor_prioridad = NULL;
//     int menor_prioridad_encontrada = INT_MAX;
    
//     for(int i = 0; i < list_size(workers_conectados); i++){
//         t_worker* w = list_get(workers_conectados, i);
//         if(w->estado == OCUPADO){
//             // Buscamos la query que esta ejecutando el worker
//             pthread_mutex_lock(&mutex_queries);
//             for(int j = 0; j < list_size(lista_queries); j++){
//                 t_query* q = list_get(lista_queries, j);
//                 if(q->worker_asignado == w->id && q->estado == EXEC){
//                     // Si la prioridad de la query en ejecucion es mayor que la nueva
//                     // y es la menor prioridad encontrada hasta ahora, entonces es el candidato
//                     if(q->prioridad > prioridad_nueva && q->prioridad < menor_prioridad_encontrada){
//                         worker_con_menor_prioridad = w;
//                         menor_prioridad_encontrada = q->prioridad;
//                     }
//                     break;
//                 }
//             }
//             pthread_mutex_unlock(&mutex_queries);
//         }
//     }
//     return worker_con_menor_prioridad;
// }

t_worker* buscar_worker_con_menor_prioridad(int prioridad_nueva) {

    // Siempre bloquear en mismo orden para evitar deadlocks
    // pthread_mutex_lock(&mutex_workers);
    pthread_mutex_lock(&mutex_queries);

    t_worker* candidato = NULL;
    int peor_prioridad = INT_MIN;  
    // Buscamos la PEOR (numéricamente mayor)

    for(int i = 0; i < list_size(workers_conectados); i++){
        t_worker* w = list_get(workers_conectados, i);

        if(w->estado != OCUPADO)
            continue;

        // Buscamos la query que está ejecutando este worker
        for(int j = 0; j < list_size(lista_queries); j++){
            t_query* q = list_get(lista_queries, j);

            if(q->worker_asignado == w->id && q->estado == EXEC){

                // Ver si podemos desalojar: prioridad actual PEOR que la nueva
                if(q->prioridad > prioridad_nueva) {

                    // Elegir la peor de todas las desalojables
                    if(q->prioridad > peor_prioridad) {
                        peor_prioridad = q->prioridad;
                        candidato = w;
                    }
                }

                // Un worker ejecuta una sola query → ya encontramos la suya
                break;
            }
        }
    }

    pthread_mutex_unlock(&mutex_queries);
    // pthread_mutex_unlock(&mutex_workers);

    return candidato;
}



void desalojar_query_de_worker(t_worker* worker, t_query* query_nueva){
    // Buscamos la query que está ejecutando el worker
    pthread_mutex_lock(&mutex_queries);
    t_query* query_a_desalojar = NULL;
    for(int i = 0; i < list_size(lista_queries); i++){
        t_query* q = list_get(lista_queries, i);
        if(q->worker_asignado == worker->id && q->estado == EXEC){
            query_a_desalojar = q;
            break;
        }
    }
    pthread_mutex_unlock(&mutex_queries);
    if(query_a_desalojar != NULL){
        
        // Enviar comando de desalojo al worker
        uint32_t size = sizeof(int);
        payload_t* payload = payload_create(size);
        payload_add(payload, &(query_a_desalojar->id), sizeof(int));
        
        paquete_t* paquete = crear_paquete(DESALOJAR_QUERY, payload);
        enviar_paquete(worker->conexion_worker, paquete);
        payload_destroy(paquete->payload);
        liberar_paquete(paquete);
        
        // Liberamos el worker
        worker->estado = LIBRE;
        worker->query_actual = -1;
        
        // Asignamos la nueva query al worker
        sem_wait(&se_puede_asignar);
        asignar_a_worker(query_nueva, worker);

        pthread_mutex_lock(&mutex_logs);
        log_info(logger, "\n## Se desaloja la Query <%d> (<%d>) del Worker <%d> - Motivo: <%d>",
                 query_a_desalojar->id, query_a_desalojar->prioridad, worker->id, query_nueva->prioridad);
        pthread_mutex_unlock(&mutex_logs);         
    }
}

bool comparar_prioridades(void* a, void* b){
    t_query* query_a = (t_query*)a;
    t_query* query_b = (t_query*)b;
    
    // Menor numero = mayor prioridad
    return query_a->prioridad < query_b->prioridad;
}

// ********************* FIN PLANIFICADOR PIORIDADES *********************

// ********************* AGING *********************

void* hilo_aging_individual(void* puntero_a_contexto){
    t_contexto_aging* contexto = (t_contexto_aging*)puntero_a_contexto;
    t_query* q = contexto->query; // el hilo trabaja sobre una query en particular!!
    usleep(master_config.tiempo_aging * 1000); 
    if(q == NULL){
        free(contexto);
        return NULL;
    }

    pthread_mutex_lock(&mutex_queries);
    if(q->estado == READY && q->vez_en_ready == contexto->vez_de_entrada){
        // chequea que la query siga en READY después de ese tiempo Y que no haya pasado por EXEC entre medio

        if(q->prioridad>0){ //validación para evitar prioridades negativas
            int prioridad_anterior = q->prioridad; //la guardamos para el log
            q->prioridad--;

            sem_post(&sem_eventos_replanificar); //reactivar algo de plani
            pthread_mutex_lock(&mutex_logs);
            log_info(logger, "\n##<%d> Cambio de prioridad: <%d> - <%d>", 
                    q->id, prioridad_anterior, q->prioridad);
            pthread_mutex_unlock(&mutex_logs); 

            // reiniciamos el hilo (suponiendo que la query sigue en ready)
            t_contexto_aging* siguiente_contexto = malloc(sizeof(t_contexto_aging));
            siguiente_contexto->query = q; // sigue con la misma query
            siguiente_contexto->vez_de_entrada = q->vez_en_ready; // acá no tendría sentido aumentar la vez de entrada, sino que tiene que seguir coincidiendo con la query mientras siga en ready


            pthread_create(&(q->hilo_aging), NULL, hilo_aging_individual, siguiente_contexto);
            pthread_detach(q->hilo_aging);
        } 
    }  // si la query se fue a exec (y todavía no volvió) o a exit, no entra en el if y el hilo muere
    pthread_mutex_unlock(&mutex_queries);
    free(contexto);
    return NULL;
}

/*void* hilo_aging_funcion(){
    while(1){
        sleep(master_config.tiempo_aging / 1000); // Convertir ms a segundos
        aplicar_aging();
    }
}

void aplicar_aging(){
    pthread_mutex_lock(&mutex_queries);
    pthread_mutex_lock(&mutex_ready);
    
    time_t tiempo_actual = time(NULL);
    int prioridad_anterior;
    
    for(int i = 0; i < list_size(lista_queries); i++){
        t_query* q = list_get(lista_queries, i);
        if(q->estado == READY){
            // Calcular tiempo transcurrido en READY
            double tiempo_transcurrido = difftime(tiempo_actual, q->tiempo_en_ready);
            
            // Si paso el tiempo de aging aumentamos prioridad
            if(tiempo_transcurrido >= master_config.tiempo_aging / 1000.0){
                if(q->prioridad > 0){ // Nos fijamos de no reducir por debajo de 0
                    prioridad_anterior = q->prioridad; //guardamos el anterior para logearlo despues
                    q->prioridad--;
                    q->tiempo_en_ready = tiempo_actual; // Reiniciamos contador
                    
                    sem_post(&sem_eventos_replanificar);
                    pthread_mutex_lock(&mutex_logs);
                    log_info(logger, "\n##<%d> Cambio de prioridad: <%d> - <%d>", 
                             q->id, prioridad_anterior, q->prioridad);
                    pthread_mutex_unlock(&mutex_logs);         
                }
            }
        }
    }
    
    pthread_mutex_unlock(&mutex_ready);
    pthread_mutex_unlock(&mutex_queries);
}
*/
// ********************* FIN AGING *********************

// ********* FUNCIONES PARA DESCONEXION DEL QC *********

t_worker* buscar_worker_por_id(int id){
    for (int i=0; i < list_size(workers_conectados); i++){
        t_worker* w = list_get(workers_conectados, i);
        if(w->id == id) return w;
    }

    return NULL;
} // funcion auxiliar para encontrar el worker asociado a la query a cancelar

void cancelar_query(t_query* q){
    // Guardamos el id del worker para poder usarlo luego de actualizar la query
    int worker_id = q->worker_asignado;
    
    

    // Si la Query se encuentra en READY, se envia a EXIT directamente
    if(q->estado == READY){
        q->estado = EXIT;
        q->worker_asignado = -1; //para que no tenga worker asignado

        // La removemos de la cola ready
        pthread_mutex_lock(&mutex_queries);
        pthread_mutex_lock(&mutex_ready);
        list_remove_element(cola_ready->elements, q);
        pthread_mutex_unlock(&mutex_ready);
        pthread_mutex_unlock(&mutex_queries);

        // No necesitamos avisar al worker porque no está en EXEC
    }
    // Si la Query se encuentra en EXEC, se le notifica al Worker para que la desaloje
    else if (q->estado == EXEC){
        // Marcamos la query como EXIT y la removemos de exec con locks adecuados
        pthread_mutex_lock(&mutex_queries);
        q->estado = EXIT;
        q->worker_asignado = -1;
        pthread_mutex_lock(&mutex_exec);
        list_remove_element(cola_exec->elements, q);
        pthread_mutex_unlock(&mutex_exec);
        pthread_mutex_unlock(&mutex_queries);
    }

    // Finalmente, si el worker todavía existe, lo marcamos libre (buscamos por id).
    if(worker_id != -1){
        pthread_mutex_lock(&mutex_workers);
        t_worker* w = buscar_worker_por_id(worker_id);
        if (w != NULL) {
            w->estado = LIBRE;
            w->query_actual = -1;
        }
        pthread_mutex_unlock(&mutex_workers);
    }
}

//******* FIN FUNCIONES PARA DESCONEXION QC*************

//********* FUNCIONES PARA DESCONEXION WORKER**********

t_query* buscar_query_por_id(int id){
    for(int i = 0; i< list_size(lista_queries); i++){
        t_query* q = list_get(lista_queries, i);
        if(q->id == id) return q; // ver después si unifico de alguna forma estas funciones
    }
    return NULL;
}

void notificar_desconexion_a_qc(t_query* q){
    pthread_mutex_lock(&mutex_qc);
    t_qc* qc_destino = buscar_qc_asociado(q);
    pthread_mutex_unlock(&mutex_qc);


    if (qc_destino != NULL){
        payload_t* payload = payload_create(sizeof(uint32_t));
        uint32_t motivo_desconexion = DESCONEXION_WORKER;
        payload_add(payload, &motivo_desconexion, sizeof(uint32_t));
        paquete_t* paquete = crear_paquete(FINALIZACION_QUERY, payload); //QC tendría que recibir y loguear esto
        enviar_paquete(qc_destino->conexion_qc, paquete);

        
        payload_destroy(payload);
        liberar_paquete(paquete);
    }
}


t_qc* buscar_qc_asociado(t_query* q){

   for(int i = 0; i < list_size(qc_conectados); i++){
        t_qc* qc = list_get(qc_conectados, i);
        if(qc->query_asociada != NULL && qc->query_asociada->id == q->id) return qc;
    }

    return NULL;
} // esta func también la uso para enviar resultados al qc

//********* FIN FUNCIONES PARA DESCONEXION WORKER**********

//******* Lecturas y devoluciones de resultados ******

void reenviar_lectura_a_qc(t_qc* qc_destino, uint32_t query_id, char* file,char* tag, void* contenido, uint32_t tamanio){
    uint32_t tam_payload =
          sizeof(uint32_t)                          
        + sizeof(uint32_t) + strlen(file) + 1       
        + sizeof(uint32_t) + strlen(tag) + 1         
        + sizeof(uint32_t)                           
        + tamanio;                                   

    payload_t* payload = payload_create(tam_payload);

    payload_add(payload, &query_id, sizeof(uint32_t));
    payload_add_string(payload, file);
    payload_add_string(payload, tag);
    payload_add(payload, &tamanio, sizeof(uint32_t));
    payload_add(payload, contenido, tamanio);

    paquete_t* paquete = crear_paquete(RESULTADO_LECTURA, payload);

    enviar_paquete(qc_destino->conexion_qc, paquete);

    payload_destroy(payload);
    liberar_paquete(paquete);
}

void reenviar_error_a_qc(t_qc* qc_destino, uint32_t codigo_error){
    uint32_t tam_payload = sizeof(uint32_t);                                                                            

    payload_t* payload = payload_create(tam_payload);

    payload_add(payload, &codigo_error, sizeof(uint32_t));

    paquete_t* paquete = crear_paquete(FINALIZACION_QUERY, payload);

    enviar_paquete(qc_destino->conexion_qc, paquete);

    payload_destroy(payload);
    liberar_paquete(paquete);
}

void enviar_finalizacion_a_qc(t_qc* qc_destino){
    uint32_t tam_payload = sizeof(uint32_t);                                                                            

    payload_t* payload = payload_create(tam_payload);

    uint32_t codigo_finalizacion = OP_EXITOSA; //código de finalización exitoso
    payload_add(payload, &codigo_finalizacion, sizeof(uint32_t));

    paquete_t* paquete = crear_paquete(FINALIZACION_QUERY, payload);

    enviar_paquete(qc_destino->conexion_qc, paquete);

    payload_destroy(payload);
    liberar_paquete(paquete);
}