# Pod TPE2

## Requisitos
* Java 21.

* Maven.

## Instalación

* Ubicarse dentro de la carpeta del proyecto.

* Compilar el proyecto ejecutando el siguiente comando:

```bash
mvn clean package
```

Después de la compilación, se generarán los siguientes archivos:

tpe2-g12-client-1.0-SNAPSHOT-bin.tar.gz: Contiene los clientes del sistema.

tpe2-g12-server-1.0-SNAPSHOT-bin.tar.gz: Contiene el servidor del sistema.

## Ejecución
### Servidor

Para la ejecución del servidor, debemos seguir los siguientes pasos:

* Ir al directorio del target:

```bash
cd server/target
```

* Descomprimir el archivo tpe2-g12-server-1.0-SNAPSHOT-bin.tar.gz:

```bash
tar -xvf tpe2-g12-server-1.0-SNAPSHOT-bin.tar.gz
```
* Ingresar a la carpeta descomprimida:

```bash
cd tpe2-g12-server-1.0-SNAPSHOT
```
* Dar permisos de ejecución al archivo:

```bash
chmod u+x run-node.sh
```
* Ejecutar el servidor. Opcionalmente, se puede indicar la máscara de red en la que se ejecutará el clúster de Hazelcast, con el parámetro -Dmask (por defecto, la máscara es 127.0.0.*):

```bash
sh run-node.sh [-Dmask=<Máscara de red>]
```

### Cliente

Para la ejecución del cliente y de las consultas, debemos seguir los siguientes pasos:

* Ir al directorio del target:

```bash
cd client/target
```
* Descomprimir el archivo tpe2-g12-client-1.0-SNAPSHOT-bin.tar.gz:

```bash
tar -xvf tpe2-g12-client-1.0-SNAPSHOT-bin.tar.gz
```
* Ingresar a la carpeta descomprimida:

```bash
cd tpe2-g12-client-1.0-SNAPSHOT
```
* Dar permisos de ejecución a los scripts de las consultas:

```bash
chmod u+x query*.sh
```

### Ejecución de las Consultas
La aplicación resuelve un conjunto de consultas listadas a continuación. Cada consulta se ejecuta mediante un script propio y requiere parámetros específicos. Cada ejecución resuelve una sola consulta utilizando los datos obtenidos de los archivos CSV provistos (multas, infracciones y agencias).

Los resultados de cada consulta se guardarán en un archivo de salida CSV 'queryX.csv', y los tiempos de ejecución se registrarán en otro archivo de texto 'timeX.csv'.

Parámetros Comunes:
- -Daddresses: Direcciones IP y puertos de los nodos del clúster de Hazelcast, separadas por punto y coma. Ejemplo: '10.6.0.1:5701;10.6.0.2:5701'.
- -Dcity: Ciudad con la que se desea trabajar. Los valores posibles son NYC y CHI.
- -DinPath: Ruta al directorio donde se encuentran los archivos de entrada (CSV de multas, infracciones y agencias).
- -DoutPath: Ruta al directorio donde se generarán los archivos de salida (queryX.csv y timeX.txt).

#### Query 1: Total de Multas por Infracción y Agencia

Ejemplo de Invocación:

```bash
sh query1.sh -Daddresses='10.6.0.1:5701' -Dcity=NYC -DinPath=/ruta/entrada -DoutPath=/ruta/salida
```
#### Query 2: Recaudación YTD por Agencia

Ejemplo de Invocación:

```bash
sh query2.sh -Daddresses='10.6.0.1:5701' -Dcity=NYC -DinPath=/ruta/entrada -DoutPath=/ruta/salida
```
#### Query 3: Porcentaje de Patentes Reincidentes por Barrio en el Rango [from, to]

Parámetros Adicionales:

- -Dn: Entero mayor o igual a 2.
- -Dfrom: Fecha de inicio en formato DD/MM/YYYY.
- -Dto: Fecha de fin en formato DD/MM/YYYY.

Ejemplo de Invocación:

```bash
sh query3.sh -Daddresses='10.6.0.1:5701' -Dcity=NYC -DinPath=/ruta/entrada -DoutPath=/ruta/salida -Dn=2 -Dfrom=01/01/2021 -Dto=31/12/2021
```

#### Query 4: Top N Infracciones con Mayor Diferencia entre Máximos y Mínimos Montos para una Agencia

Parámetros Adicionales:

- -Dn: Entero mayor o igual a 1.
- -Dagency: Nombre de la agencia (utilizar guiones bajos _ en lugar de espacios).

Ejemplo de Invocación:

```bash
sh query4.sh -Daddresses='10.6.0.1:5701' -Dcity=NYC -DinPath=/ruta/entrada -DoutPath=/ruta/salida -Dn=3 -Dagency=DEPARTMENT_OF_TRANSPORTATION
```

## Notas Importantes

*Archivos de Datos*: Los archivos CSV deben estar en el directorio especificado en -DinPath y deben tener los nombres correspondientes:

* Si -Dcity=NYC:

  - ticketsNYC.csv
  - infractionsNYC.csv
  - agenciesNYC.csv

* Si -Dcity=CHI:

  - ticketsCHI.csv
  - infractionsCHI.csv
  - agenciesCHI.csv

*Archivos de Salida*: Los resultados y los registros de tiempo se guardarán en el directorio especificado en -DoutPath como:

* queryX.csv: Resultado de la consulta.
* timeX.txt: Registro de tiempos de ejecución.

*Formato de Fechas*: Asegúrese de utilizar el formato DD/MM/YYYY para los parámetros -Dfrom y -Dto.


*Registro de Tiempos*: El archivo timeX.txt registrará los siguientes momentos con el formato dd/MM/yyyy HH:mm:ss:SSSS:

Inicio de la lectura de los archivos de entrada.
Fin de la lectura de los archivos de entrada.
Inicio del trabajo MapReduce.
Fin del trabajo MapReduce (incluye la escritura del archivo de respuesta).
Ejemplo de Registro:

```plaintext
23/10/2024 14:43:09:0223 INFO  [main] Client - Inicio de la lectura del archivo
23/10/2024 14:43:23:0011 INFO  [main] Client - Fin de lectura del archivo
23/10/2024 14:43:23:0013 INFO  [main] Client - Inicio del trabajo map/reduce
23/10/2024 14:43:23:0490 INFO  [main] Client - Fin del trabajo map/reduce
```

