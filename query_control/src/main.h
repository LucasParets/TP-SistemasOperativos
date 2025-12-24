#ifndef MAIN_H_
#define MAIN_H_

#include "utils/utils.h"
#include "utils/cliente.h"
#include "utils/serializacion.h"

void load_qc_config(char* path);
void liberar_qc();

typedef struct {
    char* ip_master;
    char* puerto_master;
    char* log_level;
} t_qc_config;

#endif
