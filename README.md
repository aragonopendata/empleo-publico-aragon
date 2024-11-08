# empleo-publico-aragon
Repositorio relacionado con información de las convocatorias de oposiciones y concursos-oposición publicados en el BOA, BOE y boletines provinciales de Aragón.

## Pre-requisitos
Este proyecto utiliza Python 3.6.9, PostgreSQL 9.6, Airflow 1.10.12 y trabaja sobre entornos virtuales de Python. También se requiere una serie de paquetes de Python adicionales.

## Flujo de ejecución
Las fases principales de la ejecución diaria son las siguientes:

### Ingesta
El primer paso es recuperar los artículos de los boletines de las secciones relacionadas con el empleo, en diferentes formatos (PDF, XML y HTML).

### Conversión
El siguiente paso es convertir los artículos, desde cualquiera de sus formatos, a ficheros de texto plano.

### Extracción
El paso principal es la extracción de la información de los diferentes campos de las ofertas de empleo. Ésta se realiza mediante un modelo NER, expresiones regulares, extracción de información de tablas horizontales y a -90º y un modelo jerárquico final.

### Almacenamiento
En este paso se insertan las ofertas y su información a la base de datos habilitada en PostgreSQL.

### Cierres de ofertas
Por último, también se cierran las ofertas de empleo cuyas plazas hayan sido cubiertas, bien sea por nombramientos o listas de admitidos, bien sea por dejarlas sin efecto o que queden desiertas.