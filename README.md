# Scraper distribuido

## Descripción General

Implementación de un scraper de páginas web como un sistema distribuido que permite a los usuarios crawlear dado un conjunto de `urls` semillas y una profundidad de crawleo. Diseñado para ser escalable y tolerante a fallos. El sistema está basado en un modelo Master-Slave fundamentalmente, el cual es un modelo donde hay nodos del sistema que desempeñan el papel de *master* y controlan la toma de decisiones; y otros nodos actúan como *slaves* o *workers* los cuales serán llamados nodos *scrapers* y realizan las tareas que les son asignadas por el *master*. En este caso, además de los tipos de nodos mencionados, se agregó los nodos *dispatcher* los cuales actúan como clientes y son los que conocen las páginas que se desean crawlear. 

<p align='center'>
  <img width='460' heigth='300' src='https://github.com/dfg-98/dist-scraper/blob/master/assets/master-slave.png?raw=true' >
  <br>
  Modelo Master-Slave
</p>

## Instalación

Clonar el repositorio, moverse a la carpeta principal a instalar los requerimientos:

'pip install -r requirements.txt'


## Concepciones del sistema

Los nodos juegan roles especializados como se mencionó se imita el modelo de Master-Slave pero no con exactitud: Hay tres tipos de nodos, los nodos *master*, los nodos *scrapers* y los nodos *dispatcher*

- Master: coordinan la red. Mantienen una lista de otros masters y de los nodos scraper en su poder.
- Scraper: hacen el trabajo de crawling de `urls`. Se conectan a los nodos master para registrarse y recibir tareas.
- Dispatcher: tienen la solicitud de `urls` a crawliar a la red. Envían mensajes a los nodos master asignandoles las `urls`.

### Comunicación:

La comunicación en el sistema ocurre principalmente usando [ZMQ](https://pyzmq.readthedocs.io/en/latest/) con varios tipos de socketes sobre TCP. En particular se utiliza UDP para el descubrimiento de nodos.

Ls nodos *scraper* y los nodos *dispatchers*  solo requiren descubrir los nodos de tipo *master*, así mismo los nodos *masters* solo tienen que descubrir a los otros *masters*.
Los *masters* reconocen al resto de nodos cuando estos se presentan en el sistema.

Se implementó un servicio para el descubrimiento de los nodos *masters* que consiste en lo siguiente:

- Se envía un mensaje en `Broadcast` de `LOGIN` a través de toda la red usando un socket *UDP*.
- Se le añade un código `magic` al mensaje para diferenciar varias instancias del sistema.
- Los nodos que reciban este mensaje (que serán otros *masters*) responderán con un mensaje de `WELCOME` (con el código `magic`) y su dirección IP.
- El nodo que hace la petición registra la dirección del *master* recibida.

En general cuando un nodo se levanta debe hacer login al sistema, para ello el nodo en cuestión puede haber recibido como parámetro la dirección de un master o intentará descubrirlo siguiendo el protocolo antes expuesto. Una vez recibida la bienvenida al sistema dicho nodo solicitará al master la sdirecciones de los masters que mínimo contendrá su dirección.

Particularmente en el paso de login cada nodo hace lo siguiente a partir de aquí:

**Master**: 

- Inicializar un proceso para solicitar los masters registrados en el sistema, de esta forma se garantiza que todos los masters se conozcan entre ellos
- Replicar las tareas entre varios masters hasta alcanzar el máximo de replicación configurado

**Scraper** y **Dispatcher**:
- Solicitar los masters registrados y guardarlos en su registro.

En lo adelante explicaremos la comunicación entre cada tipo de Nodo en el sistema.

**Comunicación Dispatcher - Master:**

Los nodos *dispatchers* se conectan con los *masters* mediante una comunicación Request-Response, mantienen 3 procesos secundarios.
Un proceso que se encarga de encontrar nuevos masters, dicho proceso hace `ping` a los masters registrados, solicita su lista de masters y trata de conectarse a ellos, en caso de fallos trata de descubrir nuevos masters con el servicio de descubrimiento, para ello usa un socket independiente.
Otro proceso se encarga de establecer la conexión con los nodos masters mediantes el socket principal de tipo `zmq.REQ` y otro de desconectar dicho socket.
La comunicación entre estos procesos ocurre mediante colas usando varios `Lock` y `Semaphore` para coordinar el acceso.
En el proceso principal del dispatcher este envía las solicitudes de crawling y se mantiene enviandolas hasta recibir respuesta.

**Comunicación Scraper - Master:**

Los nodos *scrapers* mantienen una comunicación PUSH-PULL con los *masters*. 
Mediante esta comunicación se envían y reciben las tareas de crawling así como los resultados. Además se envían notificaciones periódicas del estado de las tareas así como de la disponibilidad de los nodos usando una comunicación Request-Response.

**Comunicación Master - Master:**

La comunicación entre masters ocurre usando el patrón Publisher-Subscriber, se usa para la replicación de las tareas
de crawling y los datos en el sistema, garantizando la tolearancia a fallos.



<p align='center'>
  <img width='460' heigth='300' src='https://github.com/dfg-98/dist-scraper/blob/master/assets/pub-sub.png?raw=true'>
  <br>
  Modelo Publish-subscribe
</p>


## Coordinación:

Coordinación débil: No hay un sincronismo estricto, los nodos pueden fallar temporal o permanentemente sin comprometer el sistema. 

## Escalabilidad:

Se puede agregar más nodos fácilmente a medida que crece la carga de trabajo, levantar más nodos *scrapers* aumenta la capacidad de cómputo distribuida.


## Tolerancia a fallos

Los nodos master se mantiene actualizados entre sí y pueden detectar fallos de otros nodos, en general se establece un límite de replicación que daría la cantidad de nodos masters que deben fallar para que el sistema se caiga.


## Flujo general 

1. Los nodos master se inician y descubren otros nodos master. Luego se inician los nodos scrapers y dispatchers.  

2. Los nodos dispatchers se conectan a un nodo master e inician sesión para registrarse en la red. 

3. Los nodos dispatchers envían mensajes al nodo master con URLs a crawl en forma de tuplas ('URL', `<ID>`, `<URL>` ). Se mantienen envíando estos mensajes hasta recibir la data correspondiente con la tarea completada.

4. El nodo master recibe las tuplas (peticiones) y:

- Replica las tareas (URLs) entre los otros nodos master según el límite de replicación  
- Almacena las tareas (URLs) en un diccionario usando el ID de la tupla como clave
- Pushean las tareas a los nodos scraper.

5. Los nodos scraper reciben las tareas (URLs) y comienzan a crawlear las URLs asignadas.

6. Mientras los scrapers realizan el crawling, reciben peticiones de verificación de los masters y envían notificaciones del estado actual de las tareas.

7. El nodo master supervisa el estado de las tareas asignadas a los workers a través de los procesos worker-attender y verificator.

8. Cuando un worker termina con una URL, notifica al nodo master y la tarea se marca como completada.

9. El nodo master devuelve el resultado de la tarea al dispatcher

10. Este proceso continúa hasta que todas las URLs solicitadas hayan sido crawleadas.

11. El dispatcher escribe en disco el arbol de url con sus contenidos.

## Evaluación del sistema distribuido

### Replicación

Se implementó una Consistency Unit (Unidad de Consistencia) que es una clase auxiliar que actúa como wraper sobre un objeto, sirve para gestionar la replicación de datos en tu sistema de la siguiente manera:

- Almacena información sobre qué nodos master están controlando ese objeto (data) en particular.

- Mantiene un conteo del número de veces que se ha accedido a ese objeto.

- Cuando el límite de accesos se alcanza, se le añade "vida" al objeto y se replica según las reglas implementadas.

- Implementa las reglas para determinar cuándo y cómo replicar el objeto según el límite de réplicas especificado.

Esto ayuda a asegurar que:

- Los datos se mantienen disponibles aun cuando nodos master caen.

- Los nodos master tienen réplicas actualizadas de los datos para responder peticiones.

- Los datos son consistentes entre las réplicas en los nodos master.

### Tolerancia a fallos

Los nodos master son los que coordinan toda la red, por lo que su falla puede afectar el funcionamiento del sistema. Sin embargo, hay varios factores que mejoran la tolerancia a fallos de estos nodos:

- Los nodos master se descubren entre sí y se mantienen actualizados, por lo que pueden detectar cuando un nodo master cae.

- Los datos son replicados entre los nodos master, por lo que aún si uno cae, otros tienen réplicas actualizadas que pueden utilizar. 

- Existen nodos master redundantes que pueden asumir el rol de un nodo master caído.

Además los nodos scraper y dispatchers son más fáciles de reemplazar, ya que su único rol es consumir tareas de los nodos master y reportar estado. Si caen, los nodos master pueden redistribuir sus tareas a otros workers. El uso procesos separados y protocolos de comunicación PUSH-PULL y PUB-SUB para publicar, asignar, verificar y replicar tareas simplifica la lógica de cada nodo, lo que reduce la posibilidad de errores que puedan afectar la red completa.

### Escalabilidad

La implementación de procesos separados mejora la escalabilidad y tolerancia a fallos del sistema, pues permite distribuir la carga de trabajo entre los nodos y simplificar la lógica de cada proceso. 

Los nodos master implementan procesos para:

- Proceso de descubrimiento:
Este proceso se encarga de descubrir otros nodos master en la red y registrarlos. También desconecta nodos master caídos. 

- Proceso de publicación de tareas:
Este proceso asigna tareas (URLs a crawl) a los nodos worker usando colas de tareas.

- Proceso worker-attender:
Este proceso supervisa el estado de las tareas asignadas a los nodos worker, escuchando las notificaciones que envían.

- Proceso verificador:
Este proceso verifica el progreso de las tareas que realizan los nodos worker.

- Proceso de replicación: 
Este proceso replica las tareas (URLs) entre los nodos master según el límite de replicación especificado.

- Broadcast listener:
Este proceso escucha solicitudes de inicio de sesión de los nodos dispatchers y workers.


Los nodos workers también implementan procesos para:

- Enviar notificaciones periódicas sobre el estado de las tareas
- Solicitar nuevas tareas cuando terminan una
- Informar cuando una tarea se completa

Los nodos dispatchers solo necesitan implementar el proceso para enviar solicitudes de URLs a crawl.


## Cómo crawlear

El proceso de Crawling es simple, consiste en hacer una petición a la URL solicitada para obtener su contenido.
En caso de errores HTTP se almacena el contenido de la página con error.
En caso de errores de conexión se reintenta un número determinado de veces y en caso de no poder cumplir satisfactoriamente la petición se completa la tarea devolviendo como contenido el error obtenido.
