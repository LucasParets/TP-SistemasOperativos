# Master of Files

## Sistema Distribuido de Gestión de Consultas y Persistencia

**Trabajo Práctico – Sistemas Operativos (UTN FRBA)**

---

## 📌 Descripción del proyecto

**Master of Files** es una solución de software que simula un sistema distribuido complejo.  
El objetivo principal es la **gestión eficiente de peticiones** mediante la planificación de procesos, la administración de memoria segmentada por páginas y la persistencia de datos en un sistema de archivos propio.

El proyecto implementa conceptos fundamentales de **Sistemas Operativos**, incluyendo:

- Comunicación por **sockets**
- Procesamiento **multihilo**
- Planificación de corto plazo
- **Paginación a demanda**
- Gestión de **sistemas de archivos basados en bloques**

---

## 🧱 Arquitectura general

El sistema sigue una **metodología modular**, compuesto por **cuatro procesos independientes** que interactúan a través de la red:

### 🔹 Query Control
Es el punto de entrada de las peticiones (*Queries*) al sistema.  
Se encarga de enviar al Master las instrucciones a ejecutar junto con su prioridad.

### 🔹 Master
Actúa como el **orquestador y planificador central** del sistema.  
Administra los estados de las Queries (**READY**, **EXEC**, **EXIT**) utilizando algoritmos como:

- FIFO  
- Prioridades con Desalojo  
- Aging

### 🔹 Worker
Es el **ejecutor de las Queries**.  
Posee un intérprete de instrucciones y administra una **memoria interna** mediante un esquema de paginación simple a demanda, utilizando algoritmos de reemplazo como:

- LRU  
- CLOCK-M  

### 🔹 Storage
Representa el **File System del sistema**.  
Gestiona la persistencia física en bloques e implementa técnicas de **deduplicación de datos** mediante hashes MD5 y enlaces duros (*hard links*).

---

## 🚀 Características principales

### 🗂️ Planificación y Gestión (Master)

- **Algoritmos de planificación**: Soporte para FIFO y Prioridades dinámicas.
- **Mecanismo de Aging**: Evita la inanición (*starvation*) aumentando la prioridad de procesos en espera.
- **Multiprocesamiento**: Capacidad de gestionar múltiples Workers de forma simultánea.

---

### 🧠 Ejecución y Memoria (Worker)

- **Query Interpreter**: Parseo y ejecución de instrucciones como:
  - `CREATE`
  - `READ`
  - `WRITE`
  - `TAG`
  - `COMMIT`
- **Memoria Virtual**:  
  - Paginación administrada con un `malloc()` único.  
  - Soporte para páginas modificadas (*dirty pages*).

---

### 💾 Persistencia y Optimización (Storage)

- **Estructura del File System**:
  - Directorios nativos para representar *Files* y *Tags*
  - Archivos de metadatos
  - Mapas de bits (*bitmaps*)
- **Deduplicación**:
  - Uso de **MD5** para identificar bloques con contenido idéntico
  - Optimización del espacio físico mediante reutilización de bloques

---

## 🛠️ Tecnologías utilizadas

### Lenguajes y Herramientas de Desarrollo

- **C** — Lenguaje principal  
- **GCC** — Compilador  
- **Makefiles** — Automatización de compilación  

---

### Bibliotecas e Infraestructura

- **so-commons-library**  
  Biblioteca provista por la cátedra para:
  - Logs
  - Archivos de configuración
  - Estructuras de datos
- **Linux / Ubuntu** — Entorno de desarrollo y ejecución
- **POSIX Threads (pthreads)** — Concurrencia y servidores multihilo
- **Sockets TCP/IP** — Comunicación distribuida entre módulos

---
