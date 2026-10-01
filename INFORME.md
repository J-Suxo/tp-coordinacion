# Informe — TP Coordinación (Golang)

## Coordinación entre réplicas de Sum

Varias réplicas de Sum consumen de la misma cola de entrada, por lo que
los registros de un cliente quedan repartidos entre ellas. El EOF de ese
cliente llega a una sola réplica, así que hace falta propagarlo al resto
antes de que cada una reporte su total.

Se implementó un mecanismo de "testigo": el EOF viaja con un campo
`SeenBy`. Cada réplica que lo recibe por primera vez entrega sus sumas
de ese cliente a los Aggregation, se agrega a `SeenBy`, y si faltan
réplicas por verlo, vuelve a publicar el EOF en la misma cola de
entrada. El recorrido termina cuando `SeenBy` cubre las `SUM_AMOUNT`
réplicas.

## Partición hacia Aggregation

Cada fruta de cada cliente se asigna a un Aggregation mediante
`hash(client_id + fruta) % AGGREGATION_AMOUNT`, en vez de broadcast.
Cada Sum manda un mensaje `partial` a cada Aggregation por cliente, lo
que le permite a cada Aggregation contar cuántas réplicas de Sum ya
reportaron sin un EOF aparte.

## Join

El Join acumula los tops parciales de los Aggregation de un cliente y
calcula el top final sobre esa unión.

## Escalabilidad

- Clientes: el estado de Sum, Aggregation y Join está indexado por
  `client_id`, propagado en el protocolo interno.
- Volumen de datos: el prefetch de RabbitMQ reparte los mensajes entre
  réplicas de Sum a medida que llegan.
- Cantidad de réplicas: `SUM_AMOUNT` y `AGGREGATION_AMOUNT` son
  configurables.
## Protocolo interno

Formato binario TLV, en línea con el protocolo externo cliente-gateway,
en vez de JSON.
