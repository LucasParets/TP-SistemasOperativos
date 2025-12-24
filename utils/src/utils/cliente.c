#include "cliente.h"

int conectarse_a_modulo(char* nombre_servidor, char* ip, char* puerto, conexion_t handshake, t_log* logger) {
    //creamos la conexion
    int fd_modulo = crear_conexion(ip, puerto);

    if (fd_modulo < 0) {
        log_error(logger, "NO SE PUDO CONECTAR CON EL MODULO %s", nombre_servidor);
        exit(EXIT_FAILURE);
    }

    log_debug(logger, "CONECTADO A %s", nombre_servidor);

    //hacemos el handshake con el server que nos conectamos
    if(handshake_con_servidor(fd_modulo, handshake)) {
        log_error(logger, "HANDSHAKE CON %s INVALIDO!", nombre_servidor);
        exit(EXIT_FAILURE);
    }

    log_debug(logger, "HANDSHAKE CON %s EXITOSO!", nombre_servidor);

    //retornamos el socket con la conexion bidireccional
    return fd_modulo;
}

int crear_conexion(char* ip, char* puerto) {
    struct addrinfo hints, *server_info;

    //inicializamos la hints
    memset(&hints, 0, sizeof(hints));
    hints.ai_family = AF_INET;
    hints.ai_socktype = SOCK_STREAM;

    // obtenemos la direccion del server
    if(getaddrinfo(ip, puerto, &hints, &server_info)) {
        perror("Error getaddrinfo");
        freeaddrinfo(server_info);
        return ERROR;
    }

    //creamos el socket
    int fd_cliente = socket(server_info->ai_family,
                            server_info->ai_socktype,
                            server_info->ai_protocol);

    if(fd_cliente < 0) {
        perror("Error en socket()");
        freeaddrinfo(server_info);
        return ERROR;
    }
    
    //nos conectamos
    if(connect(fd_cliente, server_info->ai_addr, server_info->ai_addrlen) < 0) {
        perror("Error en connect()");
        freeaddrinfo(server_info);
        return ERROR;
    }

    //liberamos memoria
    freeaddrinfo(server_info);

    return fd_cliente;
}

int handshake_con_servidor(int socket_servidor, conexion_t handshake) {
    int32_t result;

    //mandamos handshake y para verificar si la comunicacion y la conexion es correcta
    if(send(socket_servidor, &handshake, sizeof(conexion_t), 0) < 0)
        return ERROR;
    if(recv(socket_servidor, &result, sizeof(result), MSG_WAITALL) < 0)
        return ERROR;

    return result; // Retorna 0 si el handshake es correcto
}


