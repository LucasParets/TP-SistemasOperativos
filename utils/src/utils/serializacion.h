#ifndef SERIALIZACION_H_
#define SERIALIZACION_H_

#include "utils.h"

typedef struct {
    uint32_t size; 
    uint32_t offset; 
    void* stream;    
} payload_t;

typedef struct {
	uint32_t cod_op;
	payload_t* payload;
} paquete_t;


payload_t *payload_create(uint32_t size);
paquete_t *crear_paquete(uint32_t operacion, payload_t *payloadCreado);
void payload_add(payload_t *payload, void *data, uint32_t size);
void payload_add_string(payload_t *payload, char* string);
void payload_read(payload_t *payload, void *data, uint32_t size);
char* payload_read_string(payload_t *payload);
int enviar_paquete(int socket, paquete_t *paquete);
paquete_t *recibir_paquete(int socket);
void payload_destroy(payload_t *payload);
void liberar_paquete(paquete_t *paquete);


#endif