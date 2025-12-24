#include "serializacion.h"

//Creacion del payload
payload_t *payload_create(uint32_t size) {
    payload_t *payload = malloc(sizeof(payload_t));
    payload->size = size;
    payload->offset = 0;
    payload->stream = malloc(size);
    return payload;
}

//Creacion del paquete
paquete_t *crear_paquete(uint32_t operacion, payload_t *payloadCreado) {
    paquete_t *paquete = malloc(sizeof(paquete_t));
    paquete->cod_op = operacion;
    paquete->payload = payloadCreado;
    return paquete;
}

//Funciones para agregar a payload
void payload_add(payload_t *payload, void *data, uint32_t size) {
    memcpy(payload->stream + payload->offset, data, size);
    payload->offset += size;
}

void payload_add_string(payload_t *payload, char* string) {
    uint32_t len = strlen(string) + 1; // agrego el '\0'
    payload_add(payload, &len, sizeof(len));
    payload_add(payload, string, len);
}

void payload_add_int_array(payload_t *payload, int *array, uint32_t count) {
    payload_add(payload, &count, sizeof(count));
    payload_add(payload, array, count * sizeof(int));
}

//Funciones para lectura de payload
void payload_read(payload_t *payload, void *data, uint32_t size) {
    memcpy(data, payload->stream + payload->offset, size);
    payload->offset += size;
}

char* payload_read_string(payload_t *payload) {
    uint32_t len;
    payload_read(payload, &len, sizeof(len));
    char* string = malloc(len);
    payload_read(payload, string, len);
    return string;
}

int* payload_read_int_array(payload_t *payload, uint32_t *count) {
    payload_read(payload, count, sizeof(*count));
    int *array = malloc(*count * sizeof(int));
    payload_read(payload, array, *count * sizeof(int));
    return array;
}

// Envio del paquete
int enviar_paquete(int socket, paquete_t *paquete) {
 
    uint32_t size = sizeof(paquete->cod_op) + sizeof(paquete->payload->size) + paquete->payload->size;

    //creamos un bufer para serializar el paquete
    void *a_enviar = malloc(size);
    int offset = 0;

    //serializamos el paquete
    memcpy(a_enviar + offset, &paquete->cod_op, sizeof(paquete->cod_op));
    offset += sizeof(paquete->cod_op);
    memcpy(a_enviar + offset, &paquete->payload->size, sizeof(paquete->payload->size));
    offset += sizeof(paquete->payload->size);
    memcpy(a_enviar + offset, paquete->payload->stream, paquete->payload->size);

    //enviamos finalmente el paquete
    int err = send(socket, a_enviar, size, MSG_NOSIGNAL);

    if( err == -1) {
		perror("Error al enviar paquete");
		free(a_enviar);
		return -1;
	}

    free(a_enviar);

	return 0;
}


// Recepcion de paquete
paquete_t *recibir_paquete(int socket) {
    // Recibo el código de operación del buffer
    uint32_t operacion;
    if(recv(socket, &operacion, sizeof(operacion), MSG_WAITALL) <= 0) {
		perror("Error al recibir codigo de operacion");
		return NULL;
	}

    // Recibo el tamaño del stream de datos del buffer para despues pedir memoria para almacenarlo
    uint32_t payload_size;
    if(recv(socket, &payload_size, sizeof(payload_size), MSG_WAITALL) <= 0) {
		perror("Error al recibir payload size");
		return NULL;
	}

    //creamos el payload con el tamaño que se nos especifico anteriormente
    payload_t *payload = payload_create(payload_size);
    //recibimos el stream de datos en nuestro nuevo payload
    if(recv(socket, payload->stream, payload_size, MSG_WAITALL) <= 0) {
		perror("Error al recibir el payload");
		return NULL;
	}

    //creamos un nuevo paquete y lo agregamos nuestro payload recien creado
    paquete_t *paquete = crear_paquete(operacion, payload);

    //retornamos el paquete
    return paquete;
}

//funciones para liberar memoria de paquetes
void payload_destroy(payload_t *payload) {
    free(payload->stream);
    free(payload);
}

void liberar_paquete(paquete_t *paquete) {
	free(paquete);
}