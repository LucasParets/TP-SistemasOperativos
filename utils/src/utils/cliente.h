#ifndef CLIENTE_H_
#define CLIENTE_H_

#include "utils.h"
int crear_conexion(char* ip, char* puerto);
int handshake_con_servidor(int socket_servidor, conexion_t handshake);
int conectarse_a_modulo(char* nombre_servidor, char* ip, char* puerto, conexion_t handshake, t_log* logger);

#endif
