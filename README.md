# Crawler distribuido

## Descripción General

Implementación de un crawler de páginas web como un sistema distribuido que permite a los usuarios crawlear dado un conjunto de `urls` semillas y una profundidad de crawleo. Diseñado para ser escalable y tolerante a fallos. El sistema está basado en un modelo Master-Slave fundamentalmente, el cual es un modelo donde hay nodos del sistema que desempeñan el papel de *master* y controlan la toma de decisiones; y otros nodos actúan como *slaves* o *workers* y realizan las tareas que les son asignadas por el *master*. En este caso, además de los tipos de nodos mencionados, se agregó los nodos *dispatcher* los cuales actúan como clientes y son los que conocen las páginas que se desean crawlear. 

<p align='center'>
  <img width='460' heigth='300' src='https://github.com/dfg-98/dist-scraper/blob/master/assets/pub-sub.png?raw=true' >
  <br>
  Modelo Master-Slave
</p>

## Instalación

Clonar el repositorio, moverse a la carpeta principal a instalar los requerimientos:

'pip install -r requirements.txt'


## Concepciones del sistema

Los nodos juegan roles especializados (clustering roles) como se mencionó se imita el modelo de Master-Slave pero no con exactitud: Hay tres tipos de nodos, los nodos *master*, los nodos *workers* y los nodos *dispatcher*

- Master: coordinan la red. Mantienen una lista de otros masters y de los nodos worker en su poder.
- Worker: hacen el trabajo de crawling de `urls`. Se conectan a los nodos master para registrarse y recibir tareas.
- Dispatcher: tienen la solicitud de `urls` a crawliar a la red. Envían mensajes a los nodos master asignandoles las `urls`.

Comunicación Publish-subscribe para el paso de mensajes en el sistema: Los nodos subscriber (workers) se suscriben a los nodos publishers (dispatchers) para recibir tareas de crawling. (Los workers tambien se subscriben a los masters????)

<p align='center'>
  <img width='460' heigth='300' src='https://github.com/dfg-98/dist-scraper/blob/master/assets/master-slave.png?raw=true'>
  <br>
  Modelo Publish-subscribe
</p>


Coordinación débil: No hay un sincronismo estricto, los nodos worker pueden fallar temporal o permanentemente sin comprometer el sistema. Se comunican mediante mensajes asincrónicos para coordinarse y replicar datos.

Escalable: Se puede agregar más nodos fácilmente a medida que crece la carga de trabajo.

Tolerante a fallos: Los nodos master se mantiene actualizados entre sí y pueden detectar fallos de otros nodos.


## Flujo general 

1. Los nodos master se inician y descubren otros nodos master. Luego se inician los nodos workers y dispatchers.  

2. Los nodos dispatchers se conectan a un nodo master e inician sesión para registrarse en la red. 

3. Los nodos dispatchers envían mensajes al nodo master con URLs a crawl en forma de tuplas (URL, ID).

4. El nodo master recibe las tuplas (peticiones) y:

- Replica las tareas (URLs) entre los otros nodos master según el límite de replicación  
- Almacena las tareas (URLs) en un diccionario usando el ID de la tupla como clave
- Asigna las tareas a los nodos workers usando colas de tareas  

5. Los nodos workers reciben las tareas (URLs) de la cola y comienzan a crawl las URLs asignadas.

6. Mientras los workers realizan el crawling, envían notificaciones periódicas al nodo master sobre el estado de progreso de la tarea.

7. El nodo master supervisa el estado de las tareas asignadas a los workers a través de los procesos worker-attender y verificator.

8. Cuando un worker termina con una URL, notifica al nodo master y la tarea se marca como completada.

9. Este proceso continúa hasta que todas las URLs solicitadas hayan sido crawleadas.

## Evaluación del sistema distribuido

### Replicación

Se implementó una Consistency Unit (Unidad de Consistencia) que es una clase auxiliar que sirve para gestionar la replicación de datos en tu sistema de la siguiente manera:

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

- La consistency unit se encarga de gestionar la replicación y disponibilidad de los datos entre los nodos master.

- Existen nodos master redundantes que pueden asumir el rol de un nodo master caído.

Además los nodos workers y dispatchers son más fáciles de reemplazar, ya que su único rol es consumir tareas de los nodos master y reportar estado. Si caen, los nodos master pueden redistribuir sus tareas a otros workers. El uso de colas de tareas y procesos separados para publicar, asignar y verificar tareas simplifica la lógica de cada nodo, lo que reduce la posibilidad de errores que puedan afectar la red completa.

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

??
