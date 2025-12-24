#ifndef SERVIDOR_H_
#define SERVIDOR_H_

#include "utils.h"

int escuchar_conexiones_de(char* nombre_modulos, char* puerto, t_log* logger);
int iniciar_servidor(char* puerto);
int esperar_conexion_de(conexion_t tipo_de_conexion, int socket_servidor);
conexion_t handshake_con_cliente(int socket_cliente);

#endif
