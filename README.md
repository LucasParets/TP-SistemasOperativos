Master of Files
Sistema Distribuido de Gestión de Consultas y Persistencia
Trabajo Práctico Cuatrimestral – Sistemas Operativos (UTN FRBA)

📌 Descripción del proyecto

Master of Files es una solución de software que simula un sistema distribuido complejo. El objetivo principal es la gestión eficiente de peticiones mediante la planificación de procesos, la administración de memoria segmentada por páginas y la persistencia de datos en un sistema de archivos propio.

El proyecto implementa conceptos fundamentales de sistemas operativos, incluyendo comunicación por sockets, multihilos, planificación de corto plazo, paginación a demanda y gestión de sistemas de archivos basados en bloques.

🧱 Arquitectura general
El sistema sigue una metodología modular, compuesto por cuatro procesos independientes que interactúan en red:

Query Control Es el punto de entrada de las peticiones (Queries) al sistema, enviando instrucciones y prioridades para su ejecución.

Master Actúa como el orquestador y planificador central. Administra los estados de las Queries (READY, EXEC, EXIT) utilizando algoritmos como FIFO o Prioridades con Desalojo y Aging.

Worker Es el brazo ejecutor del sistema. Posee un intérprete de instrucciones y administra una memoria interna mediante un esquema de paginación simple a demanda con algoritmos de reemplazo (LRU o CLOCK-M).

Storage Representa el File System del sistema. Gestiona la persistencia física en bloques, implementando técnicas de deduplicación de datos mediante hashes MD5 y enlaces duros (hard links).

🚀 Características principales
Planificación y Gestión (Master)

Algoritmos de Planificación: Soporte para FIFO y Prioridades dinámicas.


Mecanismo de Aging: Evita la inanición (starvation) aumentando la prioridad de procesos en espera.


Multiprocesamiento: Capacidad de gestionar múltiples Workers de forma simultánea.

Ejecución y Memoria (Worker)

Query Interpreter: Parseo y ejecución de instrucciones como CREATE, READ, WRITE, TAG y COMMIT.


Memoria Virtual: Paginación administrada con un malloc() único y soporte para archivos modificados (dirty pages).

Persistencia y Optimización (Storage)

Estructura FS: Basada en directorios nativos para representar Files y Tags, con archivos de metadatos y mapas de bits (bitmaps).


Deduplicación: Uso de MD5 para identificar bloques con contenido idéntico y optimizar el espacio físico.

🛠️ Tecnologías utilizadas
Lenguajes y Herramientas de Desarrollo

C (Lenguaje principal) 

GCC (Compilador)


Makefiles (Automatización de compilación) 

Bibliotecas e Infraestructura

so-commons-library: Biblioteca de utilidades de la cátedra para manejo de logs, configuración y estructuras de datos.


Linux/Ubuntu: Entorno de ejecución y desarrollo.


POSIX Threads: Para la implementación de servidores multihilos y concurrencia.


Sockets (TCP/IP): Para la comunicación distribuida entre módulos.
