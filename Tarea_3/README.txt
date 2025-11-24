Integrantes Grupo 13:
* Benjamín Barria 202273644-5
* Benjamín Soto 202204516-7
* Macarena del Hoyo 202104653-4

Para la correcta implementación del programa se debe tener en consideración:

*Ejecutar el comando (docker-compose up --build) con Docker abierto y en la carpeta de la tarea.

*En la carpeta "Tarea_3" en la ubicación [grupo-13/Tarea_3] se debe ejecutar el siguiente comando en cada MV:
dist049: make docker-mv1
dist050: make docker-mv2
dist051: make docker-mv3
dist052: make docker-mv4

En caso de error ejecutar el comando make docker-restart

Cosas a considerar del codigo:
* En el caso de un conflicto entre datanodes, la metodologia de resolucion que se ultilizo fue el de mayor id lexicograficamente. Por ejemplo clienteB>clienteA, entonces se queda la info del clienteB.

Supuestos utilizados:
* Los asientos de los aviones van de la letra A a la F. Y de los numeros 1 al 10.
* Existen solo 2 pistas para los aviones y estan son asignadas una vez llega un avion.
