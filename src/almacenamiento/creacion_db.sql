/* Creacion base de datos */
CREATE DATABASE empleo_publico_aragon;

\c empleo_publico_aragon

/* Creacion tabla FECHA */
CREATE TABLE FECHA (
    id int PRIMARY KEY,
    dia int NOT NULL,
    mes int NOT NULL,
    año int NOT NULL,
    diaSemana varchar NOT NULL,
    textual varchar NOT NULL
);

/* Creacion tabla CONVOCATORIA */
CREATE TABLE CONVOCATORIA (
    id serial PRIMARY KEY,
    organo_convocante varchar NOT NULL,
    titulo varchar NOT NULL,
    URI_ELI varchar,
    enlace varchar NOT NULL,
    grupo varchar,
    plazo varchar,
    rango varchar,
    id_orden varchar,
    fuente varchar NOT NULL,
    tipo varchar NOT NULL,
    datos_contacto varchar,
    num_plazas int,
    id_fecha_publicacion int NOT NULL,
    id_fecha_disposicion int NOT NULL,
    id_fecha_inicio_presentacion int,
    id_fecha_fin_presentacion int,
    CONSTRAINT fk_fecha_publicacion
        FOREIGN KEY (id_fecha_publicacion) REFERENCES FECHA (id),
    CONSTRAINT fk_fecha_disposicion
        FOREIGN KEY (id_fecha_disposicion) REFERENCES FECHA (id),
    CONSTRAINT fk_fecha_inicio_presentacion
        FOREIGN KEY (id_fecha_inicio_presentacion) REFERENCES FECHA (id),
    CONSTRAINT fk_fecha_fin_presentacion
        FOREIGN KEY (id_fecha_fin_presentacion) REFERENCES FECHA (id)
);

/* Creacion tabla PUESTO */
CREATE TABLE PUESTO (
    id serial PRIMARY KEY,
    denominacion varchar NOT NULL,
    cuerpo varchar,
    escala varchar,
    subescala varchar
);

/* Creacion tabla OFERTA */
CREATE TABLE OFERTA (
    id varchar PRIMARY KEY,
    enlace_cierre varchar,
    estado varchar,
    id_convocatoria int NOT NULL,
    id_puesto int NOT NULL,
    CONSTRAINT fk_convocatoria
        FOREIGN KEY (id_convocatoria) REFERENCES CONVOCATORIA (id),
    CONSTRAINT fk_puesto
        FOREIGN KEY (id_puesto) REFERENCES PUESTO (id)
);