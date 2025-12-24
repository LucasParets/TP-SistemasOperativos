#include <utils/utils.h>

t_log* iniciar_logger(char* path, char* name, bool is_active_console, t_log_level level) {

    t_log* nuevo_logger = log_create(path, name, is_active_console, level);

    if(nuevo_logger == NULL){
        fprintf(stderr, "[ERROR] iniciar_logger [%s]\n", path);
        exit(EXIT_FAILURE);
    }
        
    return nuevo_logger;
}

t_config* iniciar_config(char* path) {

    t_config* nuevo_config = config_create(path);

    if(nuevo_config == NULL){
        fprintf(stderr, "[ERROR] iniciar_config [%s]\n", path);
        exit(EXIT_FAILURE);
    }

    return nuevo_config;
}

void error_exit(char *message) { 
    perror(message);
    exit(ERROR);
}

instruccion string_a_instruccion(char* comando) {
    if(strcmp(comando, "CREATE") == 0) return CREATE;
    if(strcmp(comando, "TRUNCATE") == 0) return TRUNCATE;
    if(strcmp(comando, "WRITE") == 0) return WRITE;
    if(strcmp(comando, "READ") == 0) return READ;
    if(strcmp(comando, "TAG") == 0) return TAG;
    if(strcmp(comando, "COMMIT") == 0) return COMMIT;
    if(strcmp(comando, "FLUSH") == 0) return FLUSH;
    if(strcmp(comando, "DELETE") == 0) return DELETE;
    if(strcmp(comando, "END") == 0) return END;
    return -1;
}
