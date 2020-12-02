import psycopg2

# Parámetros conexión Postgres
PSQL_HOST = "localhost"
PSQL_PORT = "5432"
PSQL_USER = "postgres"
PSQL_PASS = "Postgres1"

# Crear conexión Postgres
connstr = "host=%s port=%s user=%s password=%s" % (PSQL_HOST, PSQL_PORT, PSQL_USER, PSQL_PASS)
postgres = psycopg2.connect(connstr)

#-------------------------------------------------------------------------------

# Crear tabla FECHA
def crear_tabla_fecha():
    sql = " \
        CREATE TABLE FECHA( \
            id int primary key , \
            dia int not null , \
            mes int not null , \
            año int not null, \
            diaSemana varchar not null , \
        textual varchar not null \
        )"
    cursor = postgres.cursor()
    cursor.execute(sql)
    postgres.commit()
    cursor.close()

# Crear tabla CONVOCATORIA
def crear_tabla_convocatoria():
    sql = " \
    CREATE TABLE CONVOCATORIA( \
        id serial primary key , \
        organo_convocante varchar not null, \
        titulo varchar not null , \
        URI_ELI varchar, \
        enlace varchar not null , \
        grupo varchar, \
        plazo varchar, \
        rango varchar, \
        id_orden varchar, \
        fuente varchar not null , \
        tipo varchar not null , \
        datos_contacto varchar, \
        num_plazas int, \
        id_fecha_publicacion int not null, \
            constraint fk_fecha_publicacion \
                foreign key (id_fecha_publicacion) references FECHA(id), \
        id_fecha_disposicion int not null, \
            constraint fk_fecha_disposicion \
                foreign key (id_fecha_disposicion) references FECHA(id), \
        id_fecha_inicio_presentacion int, \
            constraint fk_fecha_inicio_presentacion \
                foreign key (id_fecha_inicio_presentacion) references FECHA(id), \
        id_fecha_fin_presentacion int, \
            constraint fk_fecha_fin_presentacion \
                foreign key (id_fecha_fin_presentacion) references FECHA(id) \
    )"
    cursor = postgres.cursor()
    cursor.execute(sql)
    postgres.commit()
    cursor.close()

# Crear tabla PUESTO
def crear_tabla_puesto():
    sql = " \
        CREATE TABLE PUESTO( \
        id serial primary key , \
        denominacion varchar not null , \
        cuerpo varchar , \
        escala varchar , \
        subescala varchar \
    )"
    cursor = postgres.cursor()
    cursor.execute(sql)
    postgres.commit()
    cursor.close()

# Crear tabla OFERTA
def crear_tabla_oferta():
    sql = " \
    CREATE TABLE OFERTA( \
        id varchar primary key , \
        enlace_cierre varchar, \
        estado varchar, \
        id_convocatoria int not null , \
            constraint fk_convocatoria \
                    foreign key (id_convocatoria) references CONVOCATORIA(id), \
        id_puesto int not null , \
            constraint fk_puesto \
                    foreign key (id_puesto) references PUESTO(id) \
    )"
    cursor = postgres.cursor()
    cursor.execute(sql)
    postgres.commit()
    cursor.close()

# Crear esquema completo
def crear_tablas():
    crear_tabla_fecha()
    crear_tabla_convocatoria()
    crear_tabla_puesto()
    crear_tabla_oferta()

# Generar id fecha
def genera_id_fecha(d, m, a):
    fecha = str(a) + str(m) + str(d)
    return int(fecha)

# Generar id oferta
def genera_id_oferta(conv, puesto):
    sql = "select c.fuente, c.organo_convocante, c.id_fecha_disposicion, p.denominacion \
            from convocatoria c, puesto p \
            where c.id=%s and p.id=%s"
    cursor = postgres.cursor()
    cursor.execute(sql,(conv, puesto))
    data = cursor.fetchone()
    print(data)
    cursor.close()
    r = ""
    for i in data:
        r = r + i
    return r

#-------------------------------------------------------------------------------

# Main
crear_tablas()

# Cerrar conexión Postgres
postgres.close()