#include "main.h"

t_config* config;
t_config* fs_config; 
t_config* hashes_config;
t_storage_config storage_config;
t_log* logger;
t_log* logger_de_comunicacion;
int storage_server, conexion_con_worker, CANT_BLOQ_FISICOS;
t_bitarray* bitarray_bloques;
FILE* bit_file;
int map_size;
void* mappeo_bloques;
t_dictionary* mutex_por_fs;
pthread_mutex_t* mutex_bitmap;
pthread_mutex_t mutex_log, mutex_dic_fs, mutex_workers, mutex_hashes;
pthread_mutex_t *mutex_blocks;

// void* esMayor(void* a, void* b) {
//     int int_a = *(int*)a;
//     int int_b = *(int*)b;
//     intptr_t res = (int_a > int_b); // Para que no tire warnings
//     return (void*)res;
// }

bool esMayor(void* a, void* b) {
    return *(int*)a > *(int*)b;
}

bool esMenor(void* a, void* b) {
    return *(int*)a < *(int*)b;
}

void actualizar_referencias_bloque_fisico(uint32_t query_id, char* path_bloque, int bloque_fisico){
    pthread_mutex_lock(mutex_bitmap); 
    struct stat info;
    stat(path_bloque, &info);
    int referencias = info.st_nlink; // Esto te da la cantidad de hard links al archivo que le pasaste por stat
    if(referencias > 1){
        bitarray_set_bit(bitarray_bloques, bloque_fisico);
        pthread_mutex_lock(&mutex_log);
        log_info(logger, "##<%d> - Bloque Físico Reservado - Número de Bloque: <%d>", query_id, bloque_fisico);
        pthread_mutex_unlock(&mutex_log);
    }else{
        bitarray_clean_bit(bitarray_bloques, bloque_fisico);
        pthread_mutex_lock(&mutex_log);
        log_info(logger, "##<%d> - Bloque Físico Liberado - Número de Bloque: <%d>", query_id, bloque_fisico);
        pthread_mutex_unlock(&mutex_log);
    }
    pthread_mutex_unlock(mutex_bitmap);

}

int obtener_numero_de_bloque(char* bloque){
    char** partes = string_split(bloque, "/"); // Separa la ruta por los / en 
    char* nombre_archivo = partes[string_array_size(partes) - 1];

    // nombre_archivo = "00000n.dat" o "block000n.dat"
    bool hay_substring = false; 
    char** sin_extension = string_split(nombre_archivo, ".");
    char* bloque_str = sin_extension[0]; // "00000n" o "0000n"
    if(string_contains(bloque_str, "block")){ 
        // Esto es porque atoi() permite leer chars, pero si empieza con
        // un char devuelve directamente 0, que no es valido en ningun caso a excepción si del 0000.
        bloque_str = string_substring(bloque_str, 5, 4);
        hay_substring = true;
    }
    int bloque_num = atoi(bloque_str);
    if(hay_substring){
        free(bloque_str);
    }
    string_array_destroy(partes);
    string_array_destroy(sin_extension);
    return bloque_num;
}

void crear_hardlink(uint32_t query_id, char* file, char* tag, char* bloque_logico, char* bloque_fisico){
    pthread_mutex_lock(mutex_bitmap); 
    link(bloque_fisico, bloque_logico);
    int logico = obtener_numero_de_bloque(bloque_logico);
    int fisico = obtener_numero_de_bloque(bloque_fisico);
    pthread_mutex_unlock(mutex_bitmap);       
    pthread_mutex_lock(&mutex_log);
    log_info(logger, "##<%d> - <%s>:<%s> Se agregó el hard link del bloque lógico <%d> al bloque físico <%d>", query_id, file, tag, logico, fisico);
    pthread_mutex_unlock(&mutex_log);
}

void eliminar_hardlink(uint32_t query_id, char* file, char* tag, char* bloque_logico, char* bloque_fisico){
    pthread_mutex_lock(mutex_bitmap); 
    int logico = obtener_numero_de_bloque(bloque_logico);
    int fisico = obtener_numero_de_bloque(bloque_fisico);
    unlink(bloque_logico);
    pthread_mutex_lock(&mutex_log);
    log_info(logger, "##<%d> - <%s>:<%s> Se elimino el hard link del bloque lógico <%d> al bloque físico <%d>", query_id, file, tag, logico, fisico);
    pthread_mutex_unlock(&mutex_log);
    pthread_mutex_unlock(mutex_bitmap); 
}


bool ya_existe_el_dir(char* dir){
    DIR* directorio = opendir(dir);
    if (directorio != NULL) {
        closedir(directorio); // 
        return true;
    }
    return false;
}

int crear_nuevo_file(char* file, char* tag){
    char* path = string_from_format("%s/%s/%s", files_path, file, tag);    
    if(ya_existe_el_dir(path)){
        free(path);
        return FILE_TAG_PREEXISTENTE;
    }
    char* ruta_file = string_from_format("%s/%s", files_path, file);
    mkdir(ruta_file, 0777);
    char* ruta_tag = string_from_format("%s/%s/%s", files_path, file, tag);
    mkdir(ruta_tag, 0777);
    char* meta = string_from_format("%s/metadata.config", path);
    FILE* initial = fopen(meta, "w");
    fclose(initial);
    char* ruta_lb = string_from_format("%s/logical_blocks", path); 
    mkdir(ruta_lb, 0777);
    t_config* meta_config = iniciar_config(meta);
    config_set_value(meta_config, "TAMAÑO", "0");
    config_set_value(meta_config, "ESTADO", "WORK_IN_PROGRESS");   
    config_set_value(meta_config, "BLOCKS", "[]");
    config_save(meta_config);
    config_destroy(meta_config);
    char* fs = string_from_format("%s:%s", file, tag);
    pthread_mutex_t* mutex_fs = malloc(sizeof(pthread_mutex_t));
    pthread_mutex_init(mutex_fs, NULL);
    pthread_mutex_lock(&mutex_dic_fs);
    dictionary_put(mutex_por_fs, fs, mutex_fs);
    pthread_mutex_unlock(&mutex_dic_fs);
    free(fs);
    free(path);
    free(ruta_file);
    free(ruta_tag);
    free(ruta_lb);
    free(meta);
    return OP_EXITOSA;
}


t_list* obtener_bloques_logicos_de(char* bloques){
    DIR* dir = opendir(bloques);
    t_list* lista_bloques = list_create();
    struct dirent* entrada;
    if(dir == NULL){return lista_bloques;}
    while ((entrada = readdir(dir)) != NULL) { //Te va dando los archivos o directorios hasta que sea null.  
        if (strcmp(entrada->d_name, ".") == 0 || strcmp(entrada->d_name, "..") == 0) { 
            //El . es el directorio actual y el .. es el directorio padre
            continue;
        }

        int* bloque = malloc(sizeof(int));
        *bloque = atoi(entrada->d_name);
        list_add(lista_bloques, bloque);  
    }
    closedir(dir);
    list_sort(lista_bloques, esMenor);
    return lista_bloques;
}


// int obtener_cantidad_de_archivos_logicos(char* bloques){
//    DIR* dir = opendir(bloques);
//     struct dirent* entrada;
//     int cantidad_archivos;
//     int ultimo_bloque = 0;
//     if(dir == NULL){return ultimo_bloque;}
//     while ((entrada = readdir(dir)) != NULL) { //Te va dando los archivos o directorios hasta que sea null.  
//         int actual = atoi(entrada->d_name);
//         if(actual > ultimo_bloque){
//             cantidad_archivos++;
//         }
//     }
//     closedir(dir);
//     return cantidad_archivos;
// }




void deduplicar_bloque_logico(uint32_t query_id, char* file, char* tag, char* logico, char* fisico, char* nuevo_fisico){
    int indice_logico = obtener_numero_de_bloque(logico);
    int indice_fisico = obtener_numero_de_bloque(fisico); 
    int indice_fisico_nuevo = obtener_numero_de_bloque(nuevo_fisico);
    eliminar_hardlink(query_id, file, tag, logico, fisico);
    actualizar_referencias_bloque_fisico(query_id, fisico, indice_fisico);
    crear_hardlink(query_id, file, tag, logico, nuevo_fisico);
    actualizar_referencias_bloque_fisico(query_id, nuevo_fisico, indice_fisico_nuevo);
    pthread_mutex_lock(&mutex_log);
    msync(mappeo_bloques, map_size, MS_SYNC);
    log_info(logger, "##<%d> - <%s>:<%s> Bloque Lógico <%d> se reasigna de <%d> a <%d>", query_id, file, tag, indice_logico, indice_fisico, indice_fisico_nuevo);
    pthread_mutex_unlock(&mutex_log);
}

//Antes lo hacia asi, hasta que me di cuenta que el BLOCKS de metadata esta ordenado para justamente esto...
// Ademas, estaba suponiendo que no estaban los bloques logicos en orden, sino que habia que asociarlo por fcb
// char* obtener_path_bloque_fisico_asociado(char* bloque_logico, int* numero_bloque){
//     // Esta función te retorna el path del bloque físico el cual referencia el bloque lógico que se pasa por parametro
//     // Pero tambien si se quiere, te puede asignar el numero de bloque como int a una variable por referencia, sino se pasa NULL
//     struct stat info_fisico;
//     struct stat info_logico; 
//     stat(bloque_logico, &info_logico);
//     int fcb = info_logico.st_ino;
//     char* bl_fisico = NULL;
//     for(int i = 0; i < CANT_BLOQ_FISICOS; i++){
//         bl_fisico = string_from_format("%s/block%04d.dat", blocks_path, i);
//         stat(bl_fisico, &info_fisico);
//         if(info_fisico.st_ino == fcb){
//             if(numero_bloque != NULL){
//                 *numero_bloque = i; 
//             } 
//             break;
//         }
//     }
//     if(bl_fisico == NULL){
//         *numero_bloque = -1;
//     }
//     return bl_fisico;
// }


// HAY QUE USAR ESTE
t_list* obtener_bloques_de(char* path){ 
    t_list* bloques_fisicos = list_create();
    char* meta = string_from_format("%s/metadata.config", path);
    t_config* meta_config = iniciar_config(meta);
    
    // --- VALIDACIÓN DE SEGURIDAD ---
    if (meta_config == NULL) {
        // Si no se pudo abrir la metadata (borrada o bloqueada),
        // liberamos el string del path y devolvemos lista vacía para no romper todo.
        free(meta);
        return bloques_fisicos; 
    }
    // -------------------------------

    char** blocks = config_get_array_value(meta_config, "BLOCKS");
    
    if (blocks != NULL) {
        for(int i = 0; i < string_array_size(blocks); i++){
            int* n = malloc(sizeof(int)); 
            *n = atoi(blocks[i]);
            list_add(bloques_fisicos, n);
        }
        string_array_destroy(blocks);
    }
    
    config_destroy(meta_config);
    free(meta);
    return bloques_fisicos;
}


void deasignar_ultimos_hard_links(uint32_t query_id, char* file, char* tag, t_list* bloques, int cantidad){
    char* logical_path = string_from_format("%s/%s/%s/logical_blocks", files_path, file, tag);
    int index = list_size(bloques) - 1; // 3 --> 0
    // bloques = [0]; i = 2; cantidad = 3; index = 0
    
    for(int i = 0; i < cantidad; i++){
        char* hard_link = string_from_format("%s/%06d.dat", logical_path, index);
        int bloque_fisico = *(int*)list_get(bloques, index);
        pthread_mutex_lock(&mutex_blocks[bloque_fisico]);
        char* fisico_path = string_from_format("%s/block%04d.dat", blocks_path, bloque_fisico);
        // char* fisico_path = obtener_path_bloque_fisico_asociado(ruta, hard_link, &bloque_fisico); //Bloque fisico a desreferenciar 
        eliminar_hardlink(query_id, file, tag, hard_link, fisico_path);
        actualizar_referencias_bloque_fisico(query_id, fisico_path, bloque_fisico);
        pthread_mutex_unlock(&mutex_blocks[bloque_fisico]);
        void* removed = list_remove(bloques, index);
        index--;
        free(removed);
        free(hard_link);
        free(fisico_path);
    }    
    msync(mappeo_bloques, map_size, MS_SYNC);
    free(logical_path);
}


void agregar_hard_links(uint32_t query_id, char* file, char* tag, int ultimo_hl, t_list* bloques, int cantidad){ 
    char* bloque0 = string_from_format("%s/block0000.dat", blocks_path);
    
    //int* block = malloc(sizeof(int));
    //*block = 0;
    
    char* bloques_path = string_from_format("%s/%s/%s/logical_blocks", files_path, file, tag);
    for(int i = 0; i < cantidad; i++){
        
        int bloque_logico_actual = ultimo_hl + i;
        
        char* nuevo_hl = string_from_format("%s/%06d.dat", bloques_path, bloque_logico_actual);
        crear_hardlink(query_id, file, tag, nuevo_hl, bloque0);

        int* block = malloc(sizeof(int));
        *block = 0;       
        list_add(bloques, block);
        free(nuevo_hl);
    }
    actualizar_referencias_bloque_fisico(query_id, bloque0, 0);
    msync(mappeo_bloques, map_size, MS_SYNC);
    free(bloques_path);
    free(bloque0);

}



void actualizar_bloques_metadata(char* ruta, t_list* lista_bloques){
    char* meta = string_from_format("%s/metadata.config", ruta);
    t_config* meta_config = iniciar_config(meta);
    char* bloques_str = string_new();
    // list_sort(lista_bloques, esMenor);
    string_append(&bloques_str, "[");

    for (int i = 0; i < list_size(lista_bloques); i++) {
        // int bloque;
        // char* ruta_logico = string_from_format("%s/logical_blocks/%06d.dat", ruta, *(int*)list_get(lista_bloques, i));
        // char* b = obtener_path_bloque_fisico_asociado(ruta_logico, &bloque);
        // char* bloque_str = string_itoa(bloque);
        char* bloque_str = string_itoa(*(int*)list_get(lista_bloques, i)); 
        string_append(&bloques_str, bloque_str);
        if (i < list_size(lista_bloques) - 1) { //Me agrega la coma mientras no este en la ultima posición
            string_append(&bloques_str, ",");
        }
        // free(ruta_logico);
        // free(b);
        free(bloque_str);
    }
    string_append(&bloques_str, "]");
    config_set_value(meta_config, "BLOCKS", bloques_str);
    config_save(meta_config);
    config_destroy(meta_config); 
    free(meta);
    free(bloques_str);
}

int truncar_archivo(uint32_t query_id, char* file, char* tag, int nuevo_tamaño){
    char* archivo = string_from_format("%s/%s/%s", files_path, file, tag);
    
    if(!ya_existe_el_dir(archivo)){
        free(archivo);
        return FILE_TAG_INEXISTENTE;
    } 

    char* fs = string_from_format("%s:%s", file, tag);
    
    pthread_mutex_lock(&mutex_dic_fs);
    pthread_mutex_t* mutex_fs = (pthread_mutex_t*)dictionary_get(mutex_por_fs, fs);   
    pthread_mutex_unlock(&mutex_dic_fs);
    
    pthread_mutex_lock(mutex_fs);

    char* meta = string_from_format("%s/metadata.config", archivo);
    t_config* meta_config = iniciar_config(meta);

    // Validación: Si falla iniciar_config (pudo ser borrado justo antes del lock)
    if(meta_config == NULL) {
        pthread_mutex_unlock(mutex_fs);
        free(archivo);
        free(meta);
        free(fs);
        return FILE_TAG_INEXISTENTE;
    }

    if(strcmp(config_get_string_value(meta_config, "ESTADO"), "COMMITED") == 0){
        free(archivo);
        free(meta);
        free(fs); // Liberar recursos
        config_destroy(meta_config);
        pthread_mutex_unlock(mutex_fs); 
        return ESCRITURA_NO_PERMITIDA;
    } 

    if(nuevo_tamaño > fs_size){
        free(archivo);
        free(meta);
        free(fs);
        config_destroy(meta_config);
        pthread_mutex_unlock(mutex_fs); 
        return LIMITE_FISICO_EXCEDIDO;
    }

    // --- LÓGICA DE TRUNCADO ---
    char* bloques_logicos = string_from_format("%s/logical_blocks", archivo);     
    t_list* lista_bloques = obtener_bloques_de(archivo);
    
    int tamaño_actual = config_get_int_value(meta_config, "TAMAÑO");
    int bloques = abs(tamaño_actual - nuevo_tamaño) / (int)(block_size); 

    if(nuevo_tamaño < tamaño_actual){ // Achicar archivo
        deasignar_ultimos_hard_links(query_id, file, tag, lista_bloques, bloques);
    } else { // Agrandar archivo
        int ultimo_hl = 0;
        if(tamaño_actual != 0 || list_size(lista_bloques) == 0){ 
            ultimo_hl = list_size(lista_bloques); 
        }
        agregar_hard_links(query_id, file, tag, ultimo_hl, lista_bloques, bloques);
    }
    char* new_size = string_itoa(nuevo_tamaño);
    config_set_value(meta_config, "TAMAÑO", new_size);
    config_save(meta_config); 
    config_destroy(meta_config); 
    
    actualizar_bloques_metadata(archivo, lista_bloques);

    // --- LIMPIEZA ---
    free(archivo);
    free(new_size);
    free(meta);
    free(bloques_logicos);
    free(fs);
    list_destroy_and_destroy_elements(lista_bloques, free);
    
    // 5. FIN ZONA CRÍTICA
    pthread_mutex_unlock(mutex_fs);

    return OP_EXITOSA;
}

int tag_de_file(uint32_t query_id, char* file, char* tag_origen,
                char* file_destino, char* tag_destino)
{
    // 1. Preparar paths básicos
    char* origen_tag = string_from_format("%s/%s/%s", files_path, file, tag_origen);
    char* destino_tag = string_from_format("%s/%s/%s", files_path, file_destino, tag_destino);
    char* destino_dir_file = string_from_format("%s/%s", files_path, file_destino);
    
    if (ya_existe_el_dir(destino_tag)) {
        free(origen_tag); free(destino_tag); free(destino_dir_file);
        return FILE_TAG_PREEXISTENTE;
    }
    if (!ya_existe_el_dir(origen_tag)) {
        free(origen_tag); free(destino_tag); free(destino_dir_file);
        return FILE_TAG_INEXISTENTE;
    }

    char* fs_key_origen = string_from_format("%s:%s", file, tag_origen);
    pthread_mutex_lock(&mutex_dic_fs);
    pthread_mutex_t* mutex_fs = dictionary_get(mutex_por_fs, fs_key_origen);
    pthread_mutex_unlock(&mutex_dic_fs);
    

    pthread_mutex_lock(mutex_fs);
    char* meta_origen = string_from_format("%s/metadata.config", origen_tag);
    t_config* meta_config_origen = iniciar_config(meta_origen);

    if(meta_config_origen == NULL){
        pthread_mutex_unlock(mutex_fs);
        free(origen_tag); free(destino_tag); free(destino_dir_file); 
        free(meta_origen); free(fs_key_origen);
        return FILE_TAG_INEXISTENTE;
    }

    int tamanio = config_get_int_value(meta_config_origen, "TAMAÑO");
    config_destroy(meta_config_origen);
    t_list* bloques = obtener_bloques_de(origen_tag);
    if (!ya_existe_el_dir(destino_dir_file))
        mkdir(destino_dir_file, 0777);
    
    mkdir(destino_tag, 0777);

    char* meta_destino = string_from_format("%s/metadata.config", destino_tag);
    FILE* meta_file = fopen(meta_destino, "w"); // Crear archivo vacío
    fclose(meta_file);

    char* destino_lb = string_from_format("%s/logical_blocks", destino_tag);
    mkdir(destino_lb, 0777);

    t_config* meta_config_dest = iniciar_config(meta_destino);

    // Copia de Hardlinks
    for (int i = 0; i < list_size(bloques); i++) {
        int bloque_logico = i;
        int bloque_fisico = *(int*)list_get(bloques, i);

        char* logico_dest = string_from_format("%s/%06d.dat", destino_lb, bloque_logico);
        pthread_mutex_lock(&mutex_blocks[bloque_fisico]);
        char* fisico_path = string_from_format("%s/block%04d.dat", blocks_path, bloque_fisico);
        crear_hardlink(query_id, file_destino, tag_destino, logico_dest, fisico_path);
        pthread_mutex_unlock(&mutex_blocks[bloque_fisico]);
        actualizar_referencias_bloque_fisico(query_id, fisico_path, bloque_fisico);

        free(logico_dest);
        free(fisico_path);
    }
    msync(mappeo_bloques, map_size, MS_SYNC);
    char* new_size = string_itoa(tamanio);
    config_set_value(meta_config_dest, "TAMAÑO", new_size);
    config_set_value(meta_config_dest, "ESTADO", "WORK_IN_PROGRESS");
    config_save(meta_config_dest);
    config_destroy(meta_config_dest);

    pthread_mutex_t* new_mutex = malloc(sizeof(pthread_mutex_t));
    pthread_mutex_init(new_mutex, NULL);

    char* fs_key_dest = string_from_format("%s:%s", file_destino, tag_destino);
    pthread_mutex_lock(&mutex_dic_fs);
    dictionary_put(mutex_por_fs, fs_key_dest, new_mutex);
    pthread_mutex_unlock(&mutex_dic_fs);
    free(fs_key_dest);

    // Actualizar BLOCKS en metadata destino
    actualizar_bloques_metadata(destino_tag, bloques);

    pthread_mutex_unlock(mutex_fs);

    // Limpieza
    free(new_size);
    free(origen_tag);
    free(destino_tag);
    free(destino_dir_file);
    free(destino_lb);
    free(meta_origen);
    free(meta_destino);
    free(fs_key_origen);
    list_destroy_and_destroy_elements(bloques, free);

    return OP_EXITOSA;
}



int commit_tag(uint32_t query_id, char* file, char* tag){
    char* destino = string_from_format("%s/%s/%s", files_path, file, tag);

    if(!ya_existe_el_dir(destino)){
        free(destino);
        return FILE_TAG_INEXISTENTE;
    }

    char* fs = string_from_format("%s:%s", file, tag);
    
    pthread_mutex_lock(&mutex_dic_fs);
    pthread_mutex_t* mutex_fs = (pthread_mutex_t*)dictionary_get(mutex_por_fs, fs);
    pthread_mutex_unlock(&mutex_dic_fs);

    pthread_mutex_lock(mutex_fs);

    char* meta = string_from_format("%s/metadata.config", destino);
    t_config* meta_config = iniciar_config(meta);

    if(meta_config == NULL){
        pthread_mutex_unlock(mutex_fs);
        free(destino);
        free(meta);
        free(fs);
        return FILE_TAG_INEXISTENTE;
    }

    char* estado = config_get_string_value(meta_config, "ESTADO");

    // Verificar si ya está commited
    if(strcmp(estado, "COMMITED") == 0){
        free(destino);
        free(meta);
        free(fs);
        config_destroy(meta_config);
        pthread_mutex_unlock(mutex_fs); 
        return OP_EXITOSA;
    }

    char* origen = string_from_format("%s/%s/%s", files_path, file, tag);
    char* logical_path = string_from_format("%s/%s/%s/logical_blocks", files_path, file, tag);
    
    t_list* bloques = obtener_bloques_de(origen);
    char* hashes = string_from_format("%s/blocks_hash_index.config", storage_config.punto_montaje);
    
   
    
    pthread_mutex_lock(&mutex_hashes); 
    
    t_config* hashes_config = iniciar_config(hashes);
    
    for(int i = 0; i < list_size(bloques); i++){
        // # ----------- Obtener hash del bloque lógico ------------
        void* buffer = malloc(block_size);
        memset(buffer, '0', block_size); 
        int numero_fisico = *(int*)list_get(bloques, i);
        char* path_logico = string_from_format("%s/%06d.dat", logical_path, i);
        // pthread_mutex_lock(&mutex_blocks[numero_fisico]);
        char* path_fisico = string_from_format("%s/block%04d.dat", blocks_path, numero_fisico);
        FILE* bloque_fisico = fopen(path_fisico, "rb");
        fread(buffer, block_size, 1, bloque_fisico);
        fclose(bloque_fisico);
        char *hash_actual = crypto_md5(buffer, block_size);
        
        // # ----------- Verificar si existe en hashes -------------
        if(config_has_property(hashes_config, hash_actual)){
            char* bloque_hash = config_get_string_value(hashes_config, hash_actual); 
            
            // "blockXXXX" -> substring offset 5 -> "XXXX"
            char* num_bloq = string_substring(bloque_hash, 5, 4);
            int* entero_num_bloq = malloc(sizeof(int));
            *entero_num_bloq = atoi(num_bloq);
            
            if(*entero_num_bloq != numero_fisico){
                // pthread_mutex_lock(&mutex_blocks[*entero_num_bloq]);
                char* nuevo_fisico = string_from_format("%s/%s.dat", blocks_path, bloque_hash);
                deduplicar_bloque_logico(query_id, file, tag, path_logico, path_fisico, nuevo_fisico);    
                void* replace = list_replace(bloques, i, entero_num_bloq);
                free(replace); // Liberar el int viejo de la lista
                free(nuevo_fisico);
                // pthread_mutex_unlock(&mutex_blocks[*entero_num_bloq]);
            }else{
                free(entero_num_bloq);
            }
            free(num_bloq);
            // No liberar entero_num_bloq aquí, quedó en la lista
        } else {
            char* f = string_from_format("block%04d", numero_fisico);
            config_set_value(hashes_config, hash_actual, f);
            config_save(hashes_config); // Guardamos el nuevo hash
            free(f);
        }
        // pthread_mutex_unlock(&mutex_blocks[numero_fisico]);
        free(path_logico);
        free(path_fisico);
        free(buffer);
        free(hash_actual);
    } 
    pthread_mutex_unlock(&mutex_hashes); // Fin Lock global hashes
    

    // --- FINALIZAR COMMIT ---
    config_set_value(meta_config, "ESTADO", "COMMITED");
    config_save(meta_config); 
    
    actualizar_bloques_metadata(origen, bloques);
    
    // --- LIMPIEZA ---
    free(destino);
    free(meta);
    free(origen);
    free(logical_path);
    free(fs);
    free(hashes);
    config_destroy(meta_config);
    config_destroy(hashes_config); 
    list_destroy_and_destroy_elements(bloques, free);
    
    // 4. FIN ZONA CRÍTICA
    pthread_mutex_unlock(mutex_fs);
    
    return OP_EXITOSA;
}

int cantidad_de_referencias(char* path){
    pthread_mutex_lock(mutex_bitmap); 
    struct stat info_fisico;
    stat(path, &info_fisico);
    int refs = info_fisico.st_nlink;
    pthread_mutex_unlock(mutex_bitmap);
    return refs;

}

char* devolver_path_bloque_fisico_libre(){
    pthread_mutex_lock(mutex_bitmap);
    for(int i = 0; i < CANT_BLOQ_FISICOS; i++){
        if(bitarray_test_bit(bitarray_bloques, i) == 0){
            // bitarray_set_bit(bitarray_bloques, i);
            // msync(mappeo_bloques, map_size, MS_SYNC);
            char* path_fisico = string_from_format("%s/block%04d.dat", blocks_path, i);
            pthread_mutex_unlock(mutex_bitmap);
            return path_fisico;
        }
    }
    pthread_mutex_unlock(mutex_bitmap);
    return NULL;
}

char* verificar_estado_bloque_fisico(uint32_t query_id, char* file, char* tag, t_list* bloques, int bloque_logico){
    int indice_fisico_actual = *(int*)list_get(bloques, bloque_logico);
    char* ruta_logico = string_from_format("%s/%s/%s/logical_blocks/%06d.dat", files_path, file, tag, bloque_logico);
    char* path_fisico = string_from_format("%s/block%04d.dat", blocks_path, indice_fisico_actual);
    pthread_mutex_lock(&mutex_blocks[indice_fisico_actual]);
    if(cantidad_de_referencias(path_fisico) > 1){
        char* nuevo_path = devolver_path_bloque_fisico_libre();
        if(nuevo_path == NULL){
            free(ruta_logico);
            free(path_fisico);
            return NULL;
        }
        int* n = malloc(sizeof(int));
        *n = obtener_numero_de_bloque(nuevo_path);
        void* replace = list_replace(bloques, bloque_logico, n);
        free(replace);
        deduplicar_bloque_logico(query_id, file, tag, ruta_logico, path_fisico, nuevo_path);
        free(path_fisico);
        path_fisico = strdup(nuevo_path);
        free(nuevo_path);
    }
    pthread_mutex_unlock(&mutex_blocks[indice_fisico_actual]);
    free(ruta_logico);
    return path_fisico;
}

int escribir_bloque(uint32_t query_id, char* file, char* tag, int bloque_logico, void* data){
    char* ruta = string_from_format("%s/%s/%s", files_path, file, tag);
    if(!ya_existe_el_dir(ruta)){
        free(ruta);
        return FILE_TAG_INEXISTENTE;
    }

    char* fs = string_from_format("%s:%s", file, tag);   
    pthread_mutex_lock(&mutex_dic_fs);
    pthread_mutex_t* mutex_fs = (pthread_mutex_t*)dictionary_get(mutex_por_fs, fs); 
    pthread_mutex_unlock(&mutex_dic_fs);
    
    // Seguridad adicional
    if(mutex_fs == NULL){
        free(ruta); free(fs); return FILE_TAG_INEXISTENTE;
    }

    pthread_mutex_lock(mutex_fs);

    char* meta = string_from_format("%s/%s/%s/metadata.config", files_path, file, tag);
    t_config* meta_config = iniciar_config(meta);


    if(meta_config == NULL){
        pthread_mutex_unlock(mutex_fs);
        free(ruta);
        free(meta);
        free(fs);
        return FILE_TAG_INEXISTENTE;
    }


    if(strcmp(config_get_string_value(meta_config, "ESTADO"), "COMMITED") == 0){
        free(ruta);
        free(meta);
        config_destroy(meta_config);
        free(fs); 
        pthread_mutex_unlock(mutex_fs); 
        return ESCRITURA_NO_PERMITIDA;
    }
    
    t_list* bloques = obtener_bloques_de(ruta);

    if(bloque_logico >= list_size(bloques)){
        free(ruta);
        free(meta);
        list_destroy_and_destroy_elements(bloques, free);
        free(fs); 
        config_destroy(meta_config);
        pthread_mutex_unlock(mutex_fs); 
        return ESCRITURA_FUERA_DE_LIMITE;
    }


    config_destroy(meta_config);
    free(meta);
    free(ruta);
    

    char* path_fisico = verificar_estado_bloque_fisico(query_id, file, tag, bloques, bloque_logico);
    
    if(path_fisico == NULL){
        pthread_mutex_unlock(mutex_fs);
        list_destroy_and_destroy_elements(bloques, free);
        free(path_fisico);
        free(fs);
        return ESPACIO_INSUFICIENTE;
    }
    int indice_fisico_actual = obtener_numero_de_bloque(path_fisico);
    pthread_mutex_lock(&mutex_blocks[indice_fisico_actual]);
    
    FILE* f = fopen(path_fisico, "rb+");

    if (f != NULL) {
        usleep(storage_config.retardo_acceso_bloque * 1000);
        fwrite(data, block_size, 1, f);
        fclose(f);
    }

    
    char* ruta_meta = string_from_format("%s/%s/%s", files_path, file, tag);
    actualizar_bloques_metadata(ruta_meta, bloques);
    pthread_mutex_unlock(&mutex_blocks[indice_fisico_actual]); 

    free(ruta_meta);
    free(path_fisico);

    list_destroy_and_destroy_elements(bloques, free);
    pthread_mutex_unlock(mutex_fs);
    free(fs);
    return OP_EXITOSA; 
}


int leer_bloque(uint32_t query_id, char* file, char* tag, int bloque_logico, void* buffer){
    char* ruta = string_from_format("%s/%s/%s", files_path, file, tag);  
    
    if(!ya_existe_el_dir(ruta)){
        free(ruta);
        return FILE_TAG_INEXISTENTE;
    }
 
    char* fs = string_from_format("%s:%s", file, tag);
    pthread_mutex_lock(&mutex_dic_fs);
    pthread_mutex_t* mutex_fs = (pthread_mutex_t*)dictionary_get(mutex_por_fs, fs);   
    pthread_mutex_unlock(&mutex_dic_fs);
    

    if (mutex_fs == NULL) {
        free(ruta); free(fs);
        return FILE_TAG_INEXISTENTE;
    }

    pthread_mutex_lock(mutex_fs);
    t_list* bloques = obtener_bloques_de(ruta);
    
    if(bloque_logico >= list_size(bloques)){
        free(ruta);
        free(fs);
        list_destroy_and_destroy_elements(bloques, free);
        pthread_mutex_unlock(mutex_fs);
        return LECTURA_FUERA_DE_LIMITE;
    }
    // int indice_fisico_actual = *(int*)list_get(bloques, bloque_logico);
    // pthread_mutex_lock(&mutex_blocks[indice_fisico_actual]);
    char* fisico = string_from_format("%s/block%04d.dat", blocks_path, *(int*)list_get(bloques, bloque_logico));
    FILE* f = fopen(fisico, "rb");
    
    if (f != NULL) {
        usleep(storage_config.retardo_acceso_bloque * 1000); 

        fread(buffer, block_size, 1, f); //Block size = tamaño de elemento DEL BUFFER en bytes, 1 = cantidad de elementos DEL BUFFER
        fclose(f);
    }
  
    // pthread_mutex_unlock(&mutex_blocks[indice_fisico_actual]);

    free(ruta);
    free(fisico);
    free(fs);
    list_destroy_and_destroy_elements(bloques, free);
    pthread_mutex_unlock(mutex_fs);
    return OP_EXITOSA;
}

void verificar_hash_bloque(char* path_fisico){
    pthread_mutex_lock(&mutex_hashes); 
    char* hashes = string_from_format("%s/blocks_hash_index.config", storage_config.punto_montaje);
    t_config* hashes_config = iniciar_config(hashes);
    
    void* buffer = malloc(block_size);
    memset(buffer, '0', block_size); 
    FILE* bloque_fisico = fopen(path_fisico, "rb");
    fread(buffer, block_size, 1, bloque_fisico);
    fclose(bloque_fisico);
    char *hash_actual = crypto_md5(buffer, block_size);
    
    // # ----------- Verificar si existe en hashes -------------
    if(config_has_property(hashes_config, hash_actual) && cantidad_de_referencias(path_fisico) == 2){
        config_remove_key(hashes_config, hash_actual);
    }
    config_save(hashes_config);
    config_destroy(hashes_config);
    free(hashes); 
    free(buffer);
    free(hash_actual);
    pthread_mutex_unlock(&mutex_hashes); 
}

int eliminar_tag(uint32_t query_id, char* file, char* tag){
    if(strcmp(file, "initial_file") == 0){
        return OPERACION_PROHIBIDA;
    }
    char* ft_path = string_from_format("%s/%s/%s", files_path, file, tag);
    if(!ya_existe_el_dir(ft_path)){
        free(ft_path);
        return FILE_TAG_INEXISTENTE;
    }
    
    char* bloques_fisicos = string_from_format("%s/%s/%s", files_path, file, tag);
    char* fs = string_from_format("%s:%s", file, tag);
    pthread_mutex_lock(&mutex_dic_fs);
    pthread_mutex_t* mutex_fs = (pthread_mutex_t*)dictionary_get(mutex_por_fs, fs);
    pthread_mutex_unlock(&mutex_dic_fs);
    pthread_mutex_lock(mutex_fs);
    t_list* lista_bloques = obtener_bloques_de(bloques_fisicos);
    char* logical_path = string_from_format("%s/logical_blocks", bloques_fisicos);

    for(int i = 0; i < list_size(lista_bloques); i++){
        char* hard_link = string_from_format("%s/%06d.dat", logical_path, i);
        int bloque_fisico = *(int*)list_get(lista_bloques, i);
        char* fisico_path = string_from_format("%s/block%04d.dat", blocks_path, bloque_fisico);
        verificar_hash_bloque(fisico_path);
        pthread_mutex_lock(&mutex_blocks[bloque_fisico]);
        // char* fisico_path = obtener_path_bloque_fisico_asociado(hard_link, &bloque_fisico); //Bloque fisico a desreferenciar 
        eliminar_hardlink(query_id, file, tag, hard_link, fisico_path);
        pthread_mutex_unlock(&mutex_blocks[bloque_fisico]);
        actualizar_referencias_bloque_fisico(query_id, fisico_path, bloque_fisico);
        free(hard_link);
        free(fisico_path);
    }
    msync(mappeo_bloques, map_size, MS_SYNC);
    char* meta = string_from_format("%s/%s/%s/metadata.config", files_path, file, tag);
    remove(meta);
    rmdir(logical_path);
    rmdir(ft_path);
    
    free(ft_path);
    free(logical_path);
    free(bloques_fisicos);
    free(meta);
    free(fs);
    list_destroy_and_destroy_elements(lista_bloques, free);
    pthread_mutex_unlock(mutex_fs);
    return OP_EXITOSA;
}

void inicializar_bitarray() {
    map_size = (CANT_BLOQ_FISICOS + 7) / 8;

    bit_file = fopen(bitmap_path, "r+b");

    // Si no existe, crear y truncar
    if (bit_file == NULL) {
        bit_file = fopen(bitmap_path, "w+b");
        int fildes = fileno(bit_file);
        ftruncate(fildes, map_size);
    }

    int fildes = fileno(bit_file);
    mappeo_bloques = mmap(NULL, map_size, PROT_WRITE | PROT_READ, MAP_SHARED, fildes, 0);
    bitarray_bloques = bitarray_create_with_mode(mappeo_bloques, map_size, LSB_FIRST);
}


void actualizar_diccionario_tag(char* file, char* path){
    DIR* dir = opendir(path);
    struct dirent* entrada;
    while ((entrada = readdir(dir)) != NULL) { //Te va dando los archivos o directorios hasta que sea null.  
        if (strcmp(entrada->d_name, ".") == 0 || strcmp(entrada->d_name, "..") == 0) { 
            //El . es el directorio actual y el .. es el directorio padre
            continue;
        } 
        if(entrada->d_type == DT_DIR){
            pthread_mutex_t* mutex_fs = malloc(sizeof(pthread_mutex_t));
            pthread_mutex_init(mutex_fs, NULL);
            char* fs = string_from_format("%s:%s", file, (char*)entrada->d_name);
            pthread_mutex_lock(&mutex_dic_fs);
            dictionary_put(mutex_por_fs, fs, mutex_fs); 
            pthread_mutex_unlock(&mutex_dic_fs);
            free(fs);   
        }
    }    
    closedir(dir);
}

void inicializar_sem_fs(){
    DIR* dir = opendir(files_path);
    struct dirent* entrada;
    if(dir == NULL){return;}
    while ((entrada = readdir(dir)) != NULL) { //Te va dando los archivos o directorios hasta que sea null.  
        if (strcmp(entrada->d_name, ".") == 0 || strcmp(entrada->d_name, "..") == 0) { 
            //El . es el directorio actual y el .. es el directorio padre
            continue;
        } 
        if(entrada->d_type == DT_DIR){
            char* fs = string_from_format("%s/%s", files_path, (char*)entrada->d_name);
            actualizar_diccionario_tag((char*)entrada->d_name, fs);
            free(fs);
        }  
    }    
    closedir(dir);
}


void inicializar_bloques_fisicos(){
    mkdir(blocks_path, 0777);
    for(int i = 0; i < CANT_BLOQ_FISICOS; i++){
        char* real_block = string_from_format("%s/block%04d.dat", blocks_path, i);
        FILE* f = fopen(real_block, "wb+");
        int sf = fileno(f);
        ftruncate(sf, block_size);
        if (!f) { perror("fopen"); free(real_block); continue; }
        void* buf = malloc(block_size);
        memset(buf, '0', block_size); 
        fwrite(buf, block_size, 1, f);
        fflush(f);
        fclose(f);
        free(buf);
        free(real_block);
    }
}


void eliminar_estructuras_previas(char* ruta){
    DIR* dir = opendir(ruta);
    struct dirent* entrada;
    if(dir == NULL){return;}
    while ((entrada = readdir(dir)) != NULL) { //Te va dando los archivos o directorios hasta que sea null.  
        if (strcmp(entrada->d_name, ".") == 0 || strcmp(entrada->d_name, "..") == 0) { 
            //El . es el directorio actual y el .. es el directorio padre
            continue;
        } 
        if(entrada->d_type == DT_DIR){
            char* nueva_ruta = string_from_format("%s/%s", ruta, entrada->d_name);
            eliminar_estructuras_previas(nueva_ruta);
            rmdir(nueva_ruta);
            free(nueva_ruta);
        }else{
            char* archivo = string_from_format("%s/%s", ruta, entrada->d_name);
            remove(archivo);
            free(archivo); 
        }   
    }    
    closedir(dir);
    rmdir(ruta);
}

void eliminar_archivos_previos(){

    for(int i = 0; i < CANT_BLOQ_FISICOS; i++){
        char* fisico = string_from_format("%s/block%04d.dat", blocks_path, i);
        remove(fisico);
        free(fisico);
    }
    remove(blocks_path);
    remove(hashes_path);
    remove(bitmap_path);
    eliminar_estructuras_previas(files_path);
}
 
void configuracion_fresh_true(){
    //Aqui el fresh es true, se eliminan archivos previos y se formatea el nuevo fs desde 0
    eliminar_archivos_previos();
    inicializar_bloques_fisicos();
    mkdir(files_path, 0777);
    char* hashes = string_from_format("%s/blocks_hash_index.config", storage_config.punto_montaje);    
    FILE* f_hash = fopen(hashes, "w");
    fclose(f_hash);
    crear_nuevo_file("initial_file", "BASE");
    char* meta = string_from_format("%s/initial_file/BASE/metadata.config", files_path);
    t_config* meta_config = iniciar_config(meta); 
    config_set_value(meta_config, "ESTADO", "WORK_IN_PROGRESS");   
    config_set_value(meta_config, "BLOCKS", "[0]");
    config_save(meta_config);
    config_destroy(meta_config);
    char* path = string_from_format("%s/initial_file/BASE/logical_blocks", files_path);
    mkdir(path, 0777);
    char* logical_block0 = string_from_format("%s/000000.dat", path);
    char* real_block0 = string_from_format("%s/block0000.dat", blocks_path);
    link(real_block0, logical_block0);
    inicializar_bitarray();
    bitarray_set_bit(bitarray_bloques, 0); //Aca estoy suponiendo que no hay un log de crear file <query> porque es fresh_start
    msync(mappeo_bloques, map_size, MS_SYNC);
    free(hashes);
    free(meta); 
    free(logical_block0);
    free(real_block0);
    free(path);
}

void inicializar_mutex_bloques() {
    mutex_blocks = malloc(sizeof(pthread_mutex_t) * CANT_BLOQ_FISICOS);

    for (int i = 0; i < CANT_BLOQ_FISICOS; i++) {
        pthread_mutex_init(&mutex_blocks[i], NULL);
    }
}

void fs_init(){
    inicializar_mutex_bloques();
    if(storage_config.fresh_start){
        configuracion_fresh_true();
    }else{
        inicializar_bitarray();
        inicializar_sem_fs();
    }
}


t_worker* crear_referencia_a_worker(int id, int socket){
    pthread_mutex_lock(&mutex_workers);
    t_worker* nuevoWorker = malloc(sizeof(t_worker)); 
    nuevoWorker->id = id;
    nuevoWorker->conexion_worker = socket;
    list_add(workers_conectados, nuevoWorker);
    pthread_mutex_unlock(&mutex_workers);
    return nuevoWorker;
}

void mandar_resultado_a_worker(int op, int id, int conexion){
    payload_t* payload = payload_create(0); 
    payload_add(payload, &id, sizeof(id));
    paquete_t* pack = crear_paquete(op, payload);

    if(enviar_paquete(conexion, pack) != OK){
        printf("fallo al enviar el paquete a memoria");
        exit(EXIT_FAILURE);
    }

    payload_destroy(payload);
    liberar_paquete(pack);
}

void eliminar_worker(t_worker* worker){
    for(int i = 0; i < list_size(workers_conectados); i++){
        t_worker* w = list_get(workers_conectados, i);
        if(w->id == worker->id){
            list_remove_and_destroy_element(workers_conectados, i, free);
            break;
        }
    }
}

void desconectar_worker(t_worker* worker_actual){
    pthread_mutex_lock(&mutex_workers);
    pthread_mutex_lock(&mutex_log);
    log_info(logger, "Se desconecta el Worker <%d> - Cantidad de Workers: <%d>", worker_actual->id, list_size(workers_conectados) - 1);
    pthread_mutex_unlock(&mutex_log);
    eliminar_worker(worker_actual);    
    pthread_mutex_unlock(&mutex_workers);
}

void* atender_workers(void* s){
    int conexion_con_worker = *(int*)s;
    free(s);
    t_worker* worker_actual = NULL;
    uint32_t op;
    uint32_t query_id, bloque; 
    
    while(1){
        paquete_t* pack = recibir_paquete(conexion_con_worker);

        char* file = NULL; 
        char* tag = NULL;
        void* buffer = NULL;

        if (pack == NULL){
            if(worker_actual != NULL){
                desconectar_worker(worker_actual);
                close(conexion_con_worker);
                pthread_exit(NULL);
                break;
            }
            exit(EXIT_FAILURE);
        }
        
        payload_t* payload_rta = NULL;
        int tam_payload = 0;

        switch (pack->cod_op)
        {
            case MANDAR_ID: 
                int id;
                payload_read(pack->payload, &id, sizeof(id));
                worker_actual = crear_referencia_a_worker(id, conexion_con_worker); 
                pthread_mutex_lock(&mutex_workers);
                pthread_mutex_lock(&mutex_log);
                log_info(logger, "##Se conecta el Worker <%d> - Cantidad de Workers: %d", worker_actual->id, list_size(workers_conectados));
                pthread_mutex_unlock(&mutex_log);
                pthread_mutex_unlock(&mutex_workers);
                break;   

            case TAM_BLOQUE_STORAGE:
                int numero;
                payload_read(pack->payload, &numero, sizeof(numero)); 
                op = OP_EXITOSA;
                tam_payload = sizeof(op) + sizeof(uint32_t);
                payload_rta = payload_create(tam_payload);
                payload_add(payload_rta, &block_size, sizeof(block_size));
                break;

            case OP_CREATE:
                payload_read(pack->payload, &query_id, sizeof(query_id));
                file = payload_read_string(pack->payload); 
                tag = payload_read_string(pack->payload);
                usleep(storage_config.retardo_operacion * 1000);
                op = crear_nuevo_file(file, tag);
                tam_payload = sizeof(op);
                payload_rta = payload_create(tam_payload);
                if(op == OP_EXITOSA){
                    pthread_mutex_lock(&mutex_log);
                    log_info(logger, "File Creado: ##<%d> - File Creado <%s>:<%s>", query_id, file, tag);
                    pthread_mutex_unlock(&mutex_log);
                }
                free(file);
                free(tag);
                break;

            case OP_TRUNCATE:
                int tamanio;
                payload_read(pack->payload, &query_id, sizeof(query_id));
                file = payload_read_string(pack->payload); 
                tag = payload_read_string(pack->payload);
                payload_read(pack->payload, &tamanio, sizeof(tamanio));
                usleep(storage_config.retardo_operacion * 1000);
                op = truncar_archivo(query_id, file, tag, tamanio);
                tam_payload = sizeof(op);
                payload_rta = payload_create(tam_payload);
                if(op == OP_EXITOSA){
                    pthread_mutex_lock(&mutex_log);
                    log_info(logger, "##<%d> - File Truncado <%s>:<%s> - Tamaño: <%d>", query_id, file, tag, tamanio);
                    pthread_mutex_unlock(&mutex_log);
                }
                free(file);
                free(tag);
                break;

            case OP_TAG:
                payload_read(pack->payload, &query_id, sizeof(query_id));
                file = payload_read_string(pack->payload);
                char* tag_origen = payload_read_string(pack->payload); 
                char* file_destino = payload_read_string(pack->payload); 
                char* tag_destino = payload_read_string(pack->payload);
                usleep(storage_config.retardo_operacion * 1000);
                op = tag_de_file(query_id, file, tag_origen, file_destino, tag_destino);
                tam_payload = sizeof(op);
                payload_rta = payload_create(tam_payload);
                if(op == OP_EXITOSA){
                    pthread_mutex_lock(&mutex_log);
                    log_info(logger, "##<%d> - Tag creado <%s>:<%s>", query_id, file, tag_destino);
                    pthread_mutex_unlock(&mutex_log);
                }
                free(file);
                free(tag_origen);
                free(file_destino);
                free(tag_destino);
                break;

            case OP_COMMIT:
                payload_read(pack->payload, &query_id, sizeof(query_id));
                file = payload_read_string(pack->payload); 
                tag = payload_read_string(pack->payload);
                usleep(storage_config.retardo_operacion * 1000);
                op = commit_tag(query_id, file, tag);
                tam_payload = sizeof(op);
                payload_rta = payload_create(tam_payload);
                if(op == OP_EXITOSA){
                    pthread_mutex_lock(&mutex_log);
                    log_info(logger, "##<%d> - Commit de File:Tag <%s>:<%s>", query_id, file, tag);
                    pthread_mutex_unlock(&mutex_log);
                }
                free(file);
                free(tag);
                break; 

            case ESCRIBIR_BLOQUE:
                payload_read(pack->payload, &query_id, sizeof(query_id));
                file = payload_read_string(pack->payload); 
                tag = payload_read_string(pack->payload);
                payload_read(pack->payload, &bloque, sizeof(bloque));
                void* contenido = malloc(block_size);
                memset(contenido, '0', block_size);
                payload_read(pack->payload, contenido, block_size); 
                usleep(storage_config.retardo_operacion * 1000);
                op = escribir_bloque(query_id, file, tag, bloque, contenido);
                tam_payload = sizeof(op);
                payload_rta = payload_create(tam_payload);
                if(op == OP_EXITOSA){
                    pthread_mutex_lock(&mutex_log);
                    log_info(logger, "<%d> - Bloque Lógico Escrito <%s>:<%s> - Número de Bloque: <%d>", query_id, file, tag, bloque);
                    pthread_mutex_unlock(&mutex_log);
                }
                free(contenido);
                free(file);
                free(tag);
                break;

            case LEER_BLOQUE:
                buffer = calloc(1, block_size);
                memset(buffer, '0', block_size);
                payload_read(pack->payload, &query_id, sizeof(query_id));
                file = payload_read_string(pack->payload); 
                tag = payload_read_string(pack->payload);
                payload_read(pack->payload, &bloque, sizeof(bloque));
                
                usleep(storage_config.retardo_operacion * 1000); 
                op = leer_bloque(query_id, file, tag, bloque, buffer);
                
                tam_payload = sizeof(uint32_t) + block_size;
                payload_rta = payload_create(tam_payload);
                payload_add(payload_rta, &block_size, sizeof(uint32_t));
                payload_add(payload_rta, buffer, block_size);
                
                if(op == OP_EXITOSA){
                    pthread_mutex_lock(&mutex_log);
                    log_info(logger, "<%d> - Bloque Lógico Leído <%s>:<%s> - Número de Bloque: <%d>", query_id, file, tag, bloque);
                    pthread_mutex_unlock(&mutex_log);
                }
                free(buffer); 
                free(file);
                free(tag);
                break; 

            case OP_DELETE:
                payload_read(pack->payload, &query_id, sizeof(query_id));
                file = payload_read_string(pack->payload); 
                tag = payload_read_string(pack->payload);
                usleep(storage_config.retardo_operacion * 1000);
                op = eliminar_tag(query_id, file, tag);
                tam_payload = sizeof(op);
                payload_rta = payload_create(tam_payload);
                if(op == OP_EXITOSA){
                    pthread_mutex_lock(&mutex_log);
                    log_info(logger, "##<%d> - Tag Eliminado <%s>:<%s>", query_id, file, tag);
                    pthread_mutex_unlock(&mutex_log);
                }
                free(file);
                free(tag);
                break;

            case DESCONEXION:
                desconectar_worker(worker_actual);
                break;
            default:
                exit(EXIT_FAILURE);
        }

        if(pack->cod_op != MANDAR_ID && pack->cod_op != DESCONEXION){
            paquete_t* pack_rta = crear_paquete(op, payload_rta);
            if(enviar_paquete(worker_actual->conexion_worker, pack_rta) != OK){
                exit(EXIT_FAILURE);
            }
            payload_destroy(pack_rta->payload);
            liberar_paquete(pack_rta);
        }
        
        payload_destroy(pack->payload);
        liberar_paquete(pack);
    }
    return NULL;
}



void* aceptar_conexiones_con_worker(){
    while(1){
        // Storage espera que WORKER se conecte
        int* conexion_con_worker = malloc(sizeof(int));
        *conexion_con_worker = esperar_conexion_de(WORKER_CON_STORAGE, storage_server);

        if(conexion_con_worker < 0) {
            // log_error(logger_de_comunicacion, "MODULO WORKER NO PUDO CONECTARSE CON STORAGE!");
            free(conexion_con_worker);
            liberar_storage();
            exit(EXIT_FAILURE);
        }
        // log_debug(logger_de_comunicacion, "MODULO WORKER CONECTO CON STORAGE EXITOSAMENTE!");       
        
        //Hilo que se va creando a medida que viene un worker y atiende sus peticiones
        pthread_create(&hilo_peticiones_worker, NULL, atender_workers, conexion_con_worker);
        pthread_detach(hilo_peticiones_worker);
    }
    return NULL;
} 

void inicializar_mutex(){
    mutex_por_fs = dictionary_create();
    mutex_bitmap = malloc(sizeof(pthread_mutex_t));
    pthread_mutex_init(mutex_bitmap, NULL);
    pthread_mutex_init(&mutex_log, NULL);
    pthread_mutex_init(&mutex_dic_fs, NULL);
    pthread_mutex_init(&mutex_workers, NULL);
}
 

int main(int argc, char* argv[]) {
    
    char* config = argv[1];
    printf("=== INICIANDO STORAGE ===\n"); 

    inicializar_mutex();

    //iniciamos variables
    workers_conectados = list_create();
    
    //iniciamos la config
    load_storage_config(config);

    //iniciamos los loggers
    logger = iniciar_logger("storage.log", "STORAGE", 1, LOG_LEVEL_INFO);
    logger_de_comunicacion = iniciar_logger("storage.log", "STORAGE", 1, LOG_LEVEL_DEBUG);
    
    //Storage inicia un servidor que escucha conexiones de WORKER
    storage_server = escuchar_conexiones_de("STORAGE y WORKER", storage_config.puerto_escucha, logger_de_comunicacion);

    //Hilo que queda esperando a que se vayan conectando concurrentemente los worker
    //Se inicia el filesystem

    fs_init();
    pthread_create(&hilo_conexion_workers, NULL, aceptar_conexiones_con_worker, NULL);
    pthread_join(hilo_conexion_workers, NULL);
    liberar_storage();
    return 0;
}

void load_storage_config(char* path) {
   

    config = iniciar_config(path);
    
    if(config == NULL) {
        fprintf(stderr, "Config invalido!\n");
        exit(EXIT_FAILURE);
    }

    storage_config.puerto_escucha = config_get_string_value(config,"PUERTO_ESCUCHA");
    storage_config.fresh_start = false;
    if(strcmp(config_get_string_value(config,"FRESH_START"), "TRUE") == 0 || strcmp(config_get_string_value(config,"FRESH_START"), "true") == 0){
        storage_config.fresh_start = true;
    }
    storage_config.punto_montaje = config_get_string_value(config,"PUNTO_MONTAJE");
    storage_config.retardo_operacion = config_get_int_value(config,"RETARDO_OPERACION");
    storage_config.retardo_acceso_bloque = config_get_int_value(config,"RETARDO_ACCESO_BLOQUE");
    storage_config.log_level = config_get_string_value(config,"LOG_LEVEL");

    //Aca se crean las rutas para los otros archivos, files y bloques
    superblock_path = string_from_format("%s/superblock.config", storage_config.punto_montaje); 
    hashes_path = string_from_format("%s/blocks_hash_index.config", storage_config.punto_montaje); 
    bitmap_path = string_from_format("%s/bitmap.bin", storage_config.punto_montaje);
    files_path = string_from_format("%s/files", storage_config.punto_montaje); 
    blocks_path = string_from_format("%s/physical_blocks", storage_config.punto_montaje);
    fs_config = iniciar_config(superblock_path); 
    
    //Ver mas adelante, TODO
    //hashes_config = iniciar_config(hashes_path);
    fs_size = config_get_int_value(fs_config, "FS_SIZE");
    block_size = config_get_int_value(fs_config, "BLOCK_SIZE");
    CANT_BLOQ_FISICOS = (int)round(fs_size / block_size);
}

void destruir_sem(void* c){
    pthread_mutex_t* mutex = (pthread_mutex_t*)c;
    pthread_mutex_destroy(mutex);
}

void liberar_mutex_bloques(){
    for (int i = 0; i < CANT_BLOQ_FISICOS; i++) {
        pthread_mutex_destroy(&mutex_blocks[i]);
    }
    free(mutex_blocks);
}
void liberar_storage() {
    //Se libera memoria final
    close(storage_server);
    close(conexion_con_worker);
    log_destroy(logger);
    log_destroy(logger_de_comunicacion);
    config_destroy(config);
    config_destroy(fs_config);
    config_destroy(hashes_config);
    list_destroy_and_destroy_elements(workers_conectados, free);
    free(superblock_path);
    free(hashes_path);
    free(bitmap_path);
    bitarray_destroy(bitarray_bloques);   // libera la estructura de bitarray
    munmap(mappeo_bloques, ceil(CANT_BLOQ_FISICOS / 8));
    pthread_mutex_destroy(mutex_bitmap);
    free(mutex_bitmap);
    dictionary_destroy_and_destroy_elements(mutex_por_fs, destruir_sem);
    liberar_mutex_bloques();
    fclose(bit_file);
}
