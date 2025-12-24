#include "servidor.h"

int escuchar_conexiones_de(char* nombre_modulos, char* puerto, t_log* logger) {
    //iniciamos el server
    int socket_servidor = iniciar_servidor(puerto);

    if (socket_servidor < 0) {
        log_error(logger, "NO SE PUDO INICIAR EL SERVIDOR PARA QUE OTROS MODULOS SE CONECTEN");
        exit(EXIT_FAILURE);
    }
    
    log_debug(logger, "SERVIDOR INICIADO: ESPERANDO LA CONEXION DE %s", nombre_modulos);

    //retornamos el socket del server y los ponemos a la escucha
    return socket_servidor;
}

int esperar_conexion_de(conexion_t tipo_de_conexion, int socket_servidor) {

    //aceptamos la conexion
    int socket_conexion = accept(socket_servidor, NULL, NULL);

    if (socket_conexion < 0) {
        perror("Error en accept()");
        close(socket_servidor);
        return ERROR;
    }

    //mandamos el handshake para verificar
    conexion_t handshake = handshake_con_cliente(socket_conexion);

    if (handshake != tipo_de_conexion) {
        perror("Error en handshake!");
        close(socket_conexion);
        close(socket_servidor);
        return ERROR;
    }

    return socket_conexion;
}

int iniciar_servidor(char* puerto) {
    struct addrinfo hints, *server_info;

    //inicializamos hints
    memset(&hints, 0, sizeof(hints));
    hints.ai_family = AF_INET;
    hints.ai_socktype = SOCK_STREAM;
    hints.ai_flags = AI_PASSIVE;

    //Obtenemos los datos del server
    if(getaddrinfo(NULL, puerto, &hints, &server_info)) {
        perror("Error en getaddrinfo");
        freeaddrinfo(server_info);
        return ERROR;
    }

    // Creamos el socket de escucha del servidor
    int socket_servidor = socket(server_info->ai_family,
                                 server_info->ai_socktype,
                                 server_info->ai_protocol);

    if(socket_servidor < 0) {
        perror("Error en socket()");
        freeaddrinfo(server_info);
        return ERROR;
    }

    uint32_t enable = 1; // Habilita el reusar address en el socket
    if (setsockopt(socket_servidor, SOL_SOCKET, SO_REUSEADDR, &enable, sizeof(uint32_t)) < 0) {
        perror("Error en setsockopt()");
        return ERROR;
    }

    // Asociamos el socket a un puerto
    if(bind(socket_servidor, server_info->ai_addr, server_info->ai_addrlen)) {
        perror("Error en bind()");
        freeaddrinfo(server_info);
        return ERROR;
    }

    // Escuchamos las conexiones entrantes
    if(listen(socket_servidor, 1)) {
        perror("Error en listen()");
        freeaddrinfo(server_info);
        return ERROR;
    }

    freeaddrinfo(server_info);

    return socket_servidor;
}

conexion_t handshake_con_cliente(int socket_cliente) {
    conexion_t tipo_handshake = HANDSHAKE_ERROR; // Enum que identifica el tipo de conexión
    int32_t rta_handshake = 0;

    if(recv(socket_cliente, &tipo_handshake, sizeof(conexion_t), MSG_WAITALL) < 0)
        return HANDSHAKE_ERROR;

    if(tipo_handshake < 0 || tipo_handshake >= HANDSHAKE_ERROR)
        rta_handshake = -1;

    ssize_t bytes_send = send(socket_cliente, &rta_handshake, sizeof(rta_handshake), 0);

    if(bytes_send != sizeof(rta_handshake) || rta_handshake != 0)
        return HANDSHAKE_ERROR;

    return tipo_handshake;
}
