#include <memoria.h>

t_memoria_interna* crear_memoria(uint32_t tam_memoria, uint32_t tam_pagina, char* algoritmo){
    t_memoria_interna* memoria = malloc(sizeof(t_memoria_interna)); //Reserva para la estructura
    memoria->tam_memoria = tam_memoria;
    memoria->tam_pagina = tam_pagina;
    memoria->cant_marcos = tam_memoria / tam_pagina;
    memoria->memoria = malloc(tam_memoria); //Reserva para la memoria

    memoria->marcos_ocupados = calloc(memoria->cant_marcos,sizeof(bool));
    memoria->tabla_paginas = dictionary_create();
    memoria->puntero_clock = 0; // inicia en el marco 0    
    memoria->algoritmo = string_a_enum(algoritmo);
    // INICIALIZACIÓN DE LA TABLA INVERSA
    memoria->tabla_inversa_marcos = calloc(memoria->cant_marcos, sizeof(t_pagina_tabla*));
    return memoria;
}
void destruir_memoria(t_memoria_interna* memoria){
    free(memoria->memoria);
    free(memoria->marcos_ocupados);
    for (int i = 0; i < memoria->cant_marcos; i++) {
        if (memoria->tabla_inversa_marcos[i] != NULL) {
            free(memoria->tabla_inversa_marcos[i]);
        }
    }
    free(memoria->tabla_inversa_marcos);
    dictionary_destroy_and_destroy_elements(memoria->tabla_paginas,destruir_tabla);
    free(memoria);
}
void destruir_tabla (void* elemento){
    t_tabla_paginas* tabla = (t_tabla_paginas*) elemento;
    free(tabla->file);
    free(tabla->tag);
    list_destroy_and_destroy_elements(tabla->paginas, free);
    free(tabla);
}
t_resultado_memoria leer_memoria(t_memoria_interna* memoria, char* file, char* tag, int nro_pagina){
    t_resultado_memoria respuesta = {0}; //inicializo en cero
    respuesta.fallo_pagina = false;
    respuesta.requiere_flush = false;
    respuesta.pagina = NULL;
    respuesta.marco_victima = -1;
    //Busco o creo la tabla
    t_tabla_paginas* tabla = traer_tabla(memoria, file, tag);
    //Busco la página en la tabla
    t_pagina* pagina = buscar_pagina(tabla, nro_pagina);
    //Si la página no está en la tabla la cargo
    if (pagina == NULL){
        pthread_mutex_lock(&mutex_memoria);
        pagina = malloc(sizeof(t_pagina));
        pagina->numero_pagina = nro_pagina;
        pagina->presente = false;
        pagina->uso = false;
        pagina->modificado = false;
        pagina->marco = -1;
        list_add(tabla->paginas,pagina);
        pthread_mutex_unlock(&mutex_memoria);
    }    
    //Si la página está en memoria actualizo su uso y el tiempo (En caso de LRU) retorno la respuesta
    if (pagina != NULL && pagina->presente){
        pthread_mutex_lock(&mutex_memoria);
        pagina->uso = true;
        temporal_destroy(pagina->tiempo); // Para LRU
        pagina->tiempo = temporal_create();
        respuesta.pagina = pagina;
        pthread_mutex_unlock(&mutex_memoria);
        return respuesta;
    }
    //En caso de fallo de página
    respuesta.fallo_pagina = true;
    //Si la página existe en la tabla pero no está presente    
    pthread_mutex_lock(&mutex_memoria);
    if (pagina != NULL && !pagina->presente ){
        //Para cargarla veo si hay marcos líbres o hay que reemplazar        
        int marco = buscar_marco_libre(memoria);
        //Si no 
        if (marco == -1){
            switch (memoria->algoritmo){
            case LRU:
                marco = buscar_marco_lru(memoria);
                break;
            case CLOCK_M:
                marco = buscar_marco_clok_m(memoria);
                if (marco == -1){
                    marco = buscar_marco_clok_m(memoria);
                }                
                break;
            }            
            //Si la victima estaba modificada corresponde flush
            t_pagina_tabla* victima = buscar_pagina_por_marco(memoria, marco);
            if (victima != NULL) {
                if (victima->pagina->modificado) {
                    respuesta.requiere_flush = true;
                }
                //free(victima); 
            }
        }
        respuesta.marco_victima = marco;   
        
    }
    pthread_mutex_unlock(&mutex_memoria);
    return respuesta;
}
t_resultado_memoria escribir_memoria(t_memoria_interna* memoria, char* file, char* tag, uint32_t nro_pagina, void* dato, uint32_t size){
    //Intento leer la página para que me la devuelva en la rta
    t_resultado_memoria respuesta = leer_memoria(memoria, file, tag, nro_pagina);
    //Si hay fallo de página
    if (respuesta.fallo_pagina){
        //No está en memoria el Query interpreter debe:
        //ver si se requiere flush, pedir a storage la página faltante
        return respuesta;
    }
    //Si la página está en memoria
    t_pagina* pagina = respuesta.pagina;
    void* destino = memoria->memoria + (pagina->marco * memoria->tam_pagina);
    pthread_mutex_lock(&mutex_memoria);
    //Limpio la página
    memset(destino, 0, memoria->tam_pagina);
    //Escribo el contenido
    memcpy(destino, dato, size <= memoria->tam_pagina ? size : memoria->tam_pagina);

    pagina->uso = true;
    pagina->modificado = true;
    pthread_mutex_unlock(&mutex_memoria);
    return respuesta;
}
uint32_t paginas_presente(t_tabla_paginas* tabla){
    uint32_t cantidad = 0;
    for (int i = 0; i < list_size(tabla->paginas); i++){
        t_pagina* pagina = list_get(tabla->paginas, i);
        if (pagina->presente){
            cantidad++;
        }        
    }
    return cantidad;
} 
void cargar_pagina(t_memoria_interna* memoria, char* file, char* tag, int nro_pagina, int marco, void* datos, uint32_t size){
    if (marco < 0 || marco > memoria->cant_marcos){
        fprintf(stderr,"ERROR: Marco %d invalido", marco);
        return;
    }
    
    t_tabla_paginas* tabla = traer_tabla(memoria, file, tag);
    if (tabla == NULL){
        return;
    }
    

    t_pagina* pagina = buscar_pagina(tabla,nro_pagina);
    if (pagina == NULL){
        
        pagina->numero_pagina = nro_pagina;
        pagina->presente = false;
        pagina->marco = -1;
        pagina->uso = false; // Agregado para consistencia
        pagina->modificado = false; // Agregado para consistencia
        pagina->tiempo = temporal_create(); // Para LRU  
        pthread_mutex_lock(&mutex_memoria);
        list_add(tabla->paginas, pagina);
        pthread_mutex_unlock(&mutex_memoria);
    }
    //limpio la página antes de copiar
    t_pagina_tabla* pagina_tabla_a_reemplazar = memoria->tabla_inversa_marcos[marco];
    if (pagina_tabla_a_reemplazar != NULL) {
       t_pagina* pagina_a_reemplazar = pagina_tabla_a_reemplazar->pagina; // Alias para mayor claridad

       // Comprobación defensiva
       if (pagina_a_reemplazar != NULL) {
           pagina_a_reemplazar->presente = false;
           pagina_a_reemplazar->marco = -1;
           
           temporal_destroy(pagina_a_reemplazar->tiempo);
           // 3. Limpieza de tabla si la página reemplazada era la última
           if (paginas_presente(pagina_tabla_a_reemplazar->tabla) == 0) { 
               // Usamos la clave de la tabla que contenía la víctima
               pthread_mutex_lock(&mutex_memoria);
               char* clave_victima = generar_clave_tabla(pagina_tabla_a_reemplazar->tabla->file, pagina_tabla_a_reemplazar->tabla->tag);
               dictionary_remove_and_destroy(memoria->tabla_paginas, clave_victima, destruir_tabla);
               pthread_mutex_unlock(&mutex_memoria);
               free(clave_victima);
           }
       }
       // Se libera ÚNICAMENTE el registro superficial t_pagina_tabla*
       free(pagina_tabla_a_reemplazar); 
       
       // Quitar la entrada de la tabla inversa
       memoria->tabla_inversa_marcos[marco] = NULL;
       
   }
          
    void* destino = memoria->memoria + (marco * memoria->tam_pagina);
    memset(destino,0,memoria->tam_pagina);
    //copio hasta el tamaño de página
    memcpy(destino, datos, size <= memoria->tam_pagina ? size: memoria->tam_pagina);

    pagina->presente = true;
    pagina->marco = marco;
    pagina->uso = true;
    pagina->modificado = false;
    pagina->tiempo = temporal_create(); // Para LRU
    memoria->marcos_ocupados[marco] = true;

    t_pagina_tabla* nuevo_registro = malloc(sizeof(t_pagina_tabla));
    nuevo_registro->tabla = tabla;
    nuevo_registro->pagina = pagina;
    memoria->tabla_inversa_marcos[marco] = nuevo_registro;

    
}
int buscar_marco_libre(t_memoria_interna* memoria){
    int marco = -1;
    //pthread_mutex_lock(&mutex_memoria);
    for (int i = 0; i < memoria->cant_marcos; i++){
        if (!memoria->marcos_ocupados[i]){
            marco = i;
            memoria->marcos_ocupados[i] = true;
            break;
        }        
    } 
    //pthread_mutex_unlock(&mutex_memoria);
    return marco;   
}
int buscar_marco_lru(t_memoria_interna* memoria){
    t_pagina* p_victima = NULL;
    t_tabla_paginas* tabla = NULL;
    int marco = -1; 

    t_list* tablas = dictionary_elements(memoria->tabla_paginas);

    for (int i = 0; i < list_size(tablas); i++){
        tabla = list_get(tablas,i);
        
        for (int j = 0; j < list_size(tabla->paginas); j++){
            t_pagina* pagina = list_get(tabla->paginas,j);            
            if (pagina->presente){            
                int64_t tiempo_pagina = temporal_gettime(pagina->tiempo);
                if (p_victima == NULL){
                    p_victima = pagina;                     
                    marco = p_victima->marco;
                } else{
                    int64_t tiempo_victima = temporal_gettime(p_victima->tiempo);
                    if (tiempo_pagina > tiempo_victima){
                        p_victima = pagina;                         
                        marco = p_victima->marco;
                    }                    
                }                                
            }   
        }        
    }    
    list_destroy(tablas);
    //pthread_mutex_unlock(&mutex_memoria);
    return marco;
    
}
int buscar_marco_clok_m(t_memoria_interna* memoria){
    int n = memoria->cant_marcos;
    int marco_respaldo = -1;
    //Recorro dos vueltas de reloj
    for (int i = 0; i < 2*n; i++){
        int vuelta = i/n; // va a dar 0 siempre la primer vuelta y 1 la segunda
        int marco_actual = memoria->puntero_clock;        
        //busco la pagina apuntada por el clock y su tabla
        t_pagina_tabla* pagina_tabla_a_modificar = memoria->tabla_inversa_marcos[marco_actual];
        if (pagina_tabla_a_modificar == NULL) {
            // Este marco no está asociado a ninguna página PRESENTE.
            // Esto NO debería pasar si 'marcos_ocupados[marco_actual]' es true,
            // pero si pasa, avanzamos el puntero y continuamos.
            memoria->puntero_clock = (marco_actual + 1) % n;
            continue; 
        }
        
        t_pagina* pagina_a_modificar = pagina_tabla_a_modificar->pagina;
        if (pagina_a_modificar != NULL){
            //Primer vuelta
            if (vuelta == 0){
                //Si no tiene el bit de uso ni el bit de modificado es la página indicada.
                if (!pagina_a_modificar->uso && !pagina_a_modificar->modificado){
                    // si no la elijo el marco como victima
                    int marco_victima = memoria->puntero_clock;
                    memoria->puntero_clock = (marco_actual+1) % n;
                    
                    return marco_victima;   
                }
                if (!pagina_a_modificar->uso && pagina_a_modificar->modificado && marco_respaldo == -1){
                    marco_respaldo = marco_actual;
                }
                
            }
            //Segunda vuelta
            else{
                if (pagina_a_modificar->uso){
                    pagina_a_modificar->uso = false;                    
                } else{ // si no elijo el marco como victima
                    int marco_victima = memoria->puntero_clock;
                    memoria->puntero_clock = (marco_actual+1) % n;
                    
                    return marco_victima;                       
                }
            }
        }
        //avanzo el clock
        //free(pagina_tabla_a_modificar); 
        memoria->puntero_clock = (memoria->puntero_clock+1) % n;          
    }
    if (marco_respaldo != -1){
        memoria->puntero_clock = (marco_respaldo+1) % n;

        return marco_respaldo;
    }
    return -1; // No encontró ninguna página, si pasa debería correr de nuevo el algoritmo
}
void mostrar_estado_pagina(t_memoria_interna* memoria){
    printf("Estado de páginas:\n");
    t_list* tablas = dictionary_elements(memoria->tabla_paginas);
    for (int i = 0; i < list_size(tablas); i++){
        t_tabla_paginas* tabla = list_get(tablas, i);
        for (int j = 0; j < list_size(tabla->paginas); j++){
            t_pagina* pagina = list_get(tabla->paginas, j);
            if (pagina->presente){
                printf("Marco %d -> Pagina: %d (U=%d, M=%d), de la tabla: %d\n",
                pagina->marco, pagina->numero_pagina,
                pagina->uso, pagina->modificado, i);
            }            
        }        
    }
    list_destroy(tablas); 
}
t_tabla_paginas* traer_tabla(t_memoria_interna* memoria, char* file, char* tag){
    //Primero se fija si existe la tabla
    char* clave = generar_clave_tabla(file,tag);
    t_tabla_paginas* tabla = NULL;

    pthread_mutex_lock(&mutex_memoria);
    tabla = dictionary_get(memoria->tabla_paginas,clave);
    //pthread_mutex_unlock(&mutex_memoria);

    if (tabla != NULL){
        pthread_mutex_unlock(&mutex_memoria);      
        free(clave);
        return tabla;
    }

    //Si no existe la crea    
    t_tabla_paginas* nueva = malloc(sizeof(t_tabla_paginas));
    nueva->file = strdup(file);
    nueva->tag = strdup(tag);
    nueva->paginas = list_create();
    //pthread_mutex_lock(&mutex_memoria);
    dictionary_put(memoria->tabla_paginas,clave,nueva);
    pthread_mutex_unlock(&mutex_memoria);
    free(clave);
    return nueva;
}
t_pagina* buscar_pagina(t_tabla_paginas* tabla, uint32_t numero_pagina){
    pthread_mutex_lock(&mutex_memoria);
    for (int i = 0; i < list_size(tabla->paginas); i++)
    {
        t_pagina* pagina = list_get(tabla->paginas,i);
        if (pagina == NULL){
            pthread_mutex_lock(&mutex_log);
            log_error(logger,"LA PAGINA ES NULL");
            pthread_mutex_unlock(&mutex_log);
            pthread_mutex_unlock(&mutex_memoria);
            return NULL;
        }
        
        if (pagina->numero_pagina == numero_pagina){
            /*if (pagina->presente){
                pagina->tiempo = (unsigned long)time(NULL);
            }*/            
            pthread_mutex_unlock(&mutex_memoria);
            return pagina;
        }        
    }
    pthread_mutex_unlock(&mutex_memoria);
    return NULL;
}
t_pagina_tabla* buscar_pagina_por_marco(t_memoria_interna* memoria, int marco){    
    if (marco < 0 || marco >= memoria->cant_marcos){
        pthread_mutex_lock(&mutex_log);
        log_error(logger,"PROBLEMAS AL BUSCAR PAGINA POR MARCO, MARCO BUSCADO ERRONEO (%d)",marco);
        pthread_mutex_unlock(&mutex_log);
        return NULL;
    }

    t_pagina_tabla* registro = memoria->tabla_inversa_marcos[marco];
    if (registro != NULL){
        return registro;
    }
    
    return NULL; //No debería pasar    
}

t_algoritmo string_a_enum(char* str){
    if (strcmp(str, "LRU") == 0){
        return LRU;
    } else{
        return CLOCK_M;
    }    
}

t_tabla_paginas* buscar_tabla_paginas(t_memoria_interna* memoria, char* file, char* tag) {
    char* clave = generar_clave_tabla(file, tag);
    t_tabla_paginas* tabla = NULL;
    pthread_mutex_lock(&mutex_memoria);
    tabla = dictionary_get(memoria->tabla_paginas,clave);   
    pthread_mutex_unlock(&mutex_memoria);
    free(clave);
    return tabla;  // No crea la tabla si no existe, a diferencia de traer_tabla
}


char* generar_clave_tabla(char* file, char* tag) {
    char* clave;
    // +1 para separador (:), +1 para el terminador nulo (\0)
    clave = malloc(strlen(file) + strlen(tag) + 2);
    if (clave == NULL) {
       // Manejo de error de malloc si es necesario
       return NULL;
    }
    sprintf(clave, "%s:%s", file, tag); // Formato: file:tag
    return clave;
}