# [SYSTEM]
import sys
import os
import logging
import psycopg2
import itertools

from xml.etree import ElementTree as ET
from pathlib import Path
from datetime import date, timedelta, datetime
from time import strptime
from dotenv import load_dotenv

# [TRACER Y LOGGER]
from opentelemetry import trace

sys.path.append(os.path.abspath('/app/ingesta-BO'))
from tracer.tracer_configurator import TracerConfigurator
from logger.logger_configurator import LoggerConfigurator

dag_id = sys.argv[-1]

tracer_configurator = TracerConfigurator(dag_id=dag_id)
tracer = tracer_configurator.get_tracer()

logger_configurator = LoggerConfigurator(name='Almacenamiento', dag_id=dag_id)
logger = logger_configurator.get_logger()


cambio_diaSemana = {
	'monday': 'Lunes',
	'tuesday': 'Martes',
	'wednesday': 'Miércoles',
	'thursday': 'Jueves',
	'friday': 'Viernes',
	'saturday': 'Sábado',
	'sunday': 'Domingo'
}

# Devuelve el número de campos de lista que no son None
def num_campos_detectados(lista):
    with tracer.start_as_current_span("num_campos_detectados"):
        logger.debug(f"Detectando número de campos en la lista: {lista}")
        campos_detectados = sum(e is not None for e in lista)
        logger.info(f"Número de campos detectados: {campos_detectados}")
        return campos_detectados

def insertar_fecha(cursor, fecha_textual):
    if fecha_textual is None:
        logger.warning("Fecha textual es None, no se insertará nada.")
        return

    with tracer.start_as_current_span("insertar_fecha") as span:
        try:
            id_fecha, dia, mes, año, dia_semana = obtener_att_fecha(fecha_textual)
            sql = ('INSERT INTO fecha(id, dia, mes, año, diasemana, textual) '
                   'VALUES(%s, %s, %s, %s, %s, %s) '
                   'ON CONFLICT DO NOTHING;')
            cursor.execute(sql, (id_fecha, dia, mes, año, dia_semana, fecha_textual))
            logger.info(f"Fecha insertada correctamente: {id_fecha}")
            span.set_attribute("fecha_textual", fecha_textual)
            return id_fecha
        except Exception as e:
            logger.exception(f"Error al insertar la fecha: {fecha_textual}")
            span.record_exception(e)
            span.set_status(trace.StatusCode.ERROR)
            raise

def hay_puesto(puesto, cursor):
    with tracer.start_as_current_span("hay_puesto") as span:
        try:
            sql = 'SELECT id FROM puesto WHERE '
            partes = ['denominacion', 'cuerpo', 'escala', 'subescala']
            nuevo_puesto = []
            for i, parte in enumerate(partes):
                sql += parte
                if puesto[i] is None:
                    sql += ' is null'
                else:
                    sql += ' = %s'
                    nuevo_puesto.append(puesto[i])
                if i < len(partes)-1:
                    sql += ' AND '
            sql += ';'

            cursor.execute(sql, nuevo_puesto)
            id_recuperado = cursor.fetchone()
            if puesto is not None:
                span.set_attribute("puesto", puesto)
            logger.debug(f"Puesto verificado: {puesto}, ID recuperado: {id_recuperado}")
            return id_recuperado[0] if id_recuperado is not None else False
        except Exception as e:
            logger.exception(f"Error al verificar el puesto: {puesto}")
            span.record_exception(e)
            span.set_status(trace.StatusCode.ERROR)
            raise

def insertar_puestos(cursor, denominaciones, cuerpo, escala, subescala):
    with tracer.start_as_current_span("insertar_puestos") as span:
        try:
            puestos_iter = itertools.product(
                denominaciones, [cuerpo], [escala], [subescala])

            ids_puestos = []
            for puesto in puestos_iter:
                id_puesto_existente = hay_puesto(puesto, cursor)
                if not id_puesto_existente:
                    sql = ('INSERT INTO puesto(denominacion, cuerpo, escala, subescala) '
                           'VALUES(%s, %s, %s, %s) '
                           'RETURNING id;')
                    cursor.execute(sql, puesto)
                    id_puesto = cursor.fetchone()[0]
                    ids_puestos.append(id_puesto)
                    logger.info(f"Puesto insertado: {puesto}")
                else:
                    ids_puestos.append(id_puesto_existente)
                    logger.info(f"Puesto ya existente: {puesto}")
            span.set_attribute("puestos_insertados", ids_puestos)
            return ids_puestos
        except Exception as e:
            logger.exception("Error al insertar puestos.")
            span.record_exception(e)
            span.set_status(trace.StatusCode.ERROR)
            raise

def insertar_convocatoria(cursor, organo_convocante, titulo, uri_eli, enlace,
                          grupo, plazo, rango, id_orden, fuente, tipo, 
                          datos_contacto, num_plazas, id_fecha_publicacion, 
                          id_fecha_disposicion, id_fecha_inicio_presentacion,
                          id_fecha_fin_presentacion):
    with tracer.start_as_current_span("insertar_convocatoria") as span:
        try:
            sql = ('INSERT INTO convocatoria('
                   'organo_convocante, titulo, URI_ELI, enlace, grupo, plazo, '
                   'rango, id_orden, fuente, tipo, datos_contacto, '
                   'num_plazas, id_fecha_publicacion, id_fecha_disposicion, '
                   'id_fecha_inicio_presentacion, id_fecha_fin_presentacion) '
                   'VALUES(%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, '
                   '%s, %s) '
                   'RETURNING id;')
            cursor.execute(sql, (organo_convocante, titulo, uri_eli, enlace, grupo,
                                 plazo, rango, id_orden, fuente, tipo,
                                 datos_contacto, num_plazas, id_fecha_publicacion,
                                 id_fecha_disposicion, id_fecha_inicio_presentacion,
                                 id_fecha_fin_presentacion))
            id_convocatoria = cursor.fetchone()[0]
            span.set_attribute("id_convocatoria", id_convocatoria)
            logger.info(f"Convocatoria insertada con ID: {id_convocatoria}")
            return id_convocatoria
        except Exception as e:
            logger.exception("Error al insertar convocatoria.")
            span.record_exception(e)
            span.set_status(trace.StatusCode.ERROR)
            raise

def insertar_oferta(cursor, estado, enlace_cierre, id_convocatoria, ids_puestos_lista):
    with tracer.start_as_current_span("insertar_oferta") as span:
        ids_ofertas_lista = []
        try:
            for id_puesto in ids_puestos_lista:
                id_oferta = generar_id_oferta(cursor, id_convocatoria, id_puesto)

                query = ('SELECT id '
                         'FROM oferta '
                         'WHERE id LIKE %s '
                         'ORDER BY cast(SPLIT_PART(id, %s, 5) AS integer) DESC;')
                cursor.execute(query, (f'{id_oferta}_%', '_'))
                ultimo_id = cursor.fetchone()

                if ultimo_id is None:
                    serial = 0
                else:
                    serial = int(ultimo_id[0].split('_')[-1])

                id_oferta = f'{id_oferta}_{serial + 1}'

                sql = ('INSERT INTO oferta(id, enlace_cierre, estado, '
                       'id_convocatoria, id_puesto) '
                       'VALUES(%s, %s, %s, %s, %s);')
                cursor.execute(sql, (id_oferta, enlace_cierre, estado,
                                     id_convocatoria, id_puesto))

                ids_ofertas_lista.append(id_oferta)
                logger.info(f"Oferta insertada con ID: {id_oferta}")

            span.set_attribute("ofertas_insertadas", ids_ofertas_lista)
            return ids_ofertas_lista
        except Exception as e:
            logger.exception("Error al insertar ofertas.")
            span.record_exception(e)
            span.set_status(trace.StatusCode.ERROR)
            raise

def generar_id_oferta(cursor, id_convocatoria, id_puesto):
    with tracer.start_as_current_span("generar_id_oferta") as span:
        try:
            query = ('SELECT c.fuente, c.organo_convocante, c.id_fecha_disposicion, p.denominacion '
                     'FROM convocatoria c, puesto p '
                     'WHERE c.id = %s AND p.id = %s')

            cursor.execute(query, (id_convocatoria, id_puesto))
            data = cursor.fetchone()
            data = list(data)
            data[2] = str(data[2])
            id_oferta = '_'.join(data)
            logger.info(f"Generated oferta id {id_oferta}")
            span.set_attribute("id_oferta", id_oferta)
        except Exception as e:
            logger.exception("Error generating oferta ID.")
            span.record_exception(e)
            span.set_status(trace.StatusCode.ERROR)
            raise

        return id_oferta

def eliminar_records(conn, cursor):
    with tracer.start_as_current_span("eliminar_records") as span:
        try:
            # Borrar registros en orden de dependencia inversa
            sql = 'DELETE FROM oferta;'
            cursor.execute(sql)
            logger.info("Deleted all records from oferta table.")
            span.set_attribute("status_oferta", "deleted")

            sql = 'DELETE FROM convocatoria;'
            cursor.execute(sql)
            logger.info("Deleted all records from convocatoria table.")
            span.set_attribute("status_convocatoria", "deleted")

            sql = 'DELETE FROM puesto;'
            cursor.execute(sql)
            logger.info("Deleted all records from puesto table.")
            span.set_attribute("status_puesto", "deleted")
            
            sql = 'DELETE FROM fecha;'
            cursor.execute(sql)
            logger.info("Deleted all records from fecha table.")
            span.set_attribute("status_fecha", "deleted")

            conn.commit()
            logger.info("All records successfully deleted and changes committed.")
            span.set_attribute("commit_status", "successful")
        except Exception as e:
            logger.exception("Error deleting records.")
            span.record_exception(e)
            span.set_status(trace.StatusCode.ERROR)
            conn.rollback()
            logger.info("Rolled back the transaction due to an error.")
            span.set_attribute("rollback_status", "executed")
            raise

def obtener_root_fichero(ruta):
    with tracer.start_as_current_span("obtener_root_fichero") as span:
        try:
            with open(ruta, 'rb') as file:
                tree = ET.parse(file)
                root = tree.getroot()
                logger.info(f"Successfully parsed XML file at {ruta}.")
                span.set_attribute("file_path", str(ruta))
                return root
        except Exception as e:
            msg = f"Failed to read or parse XML file at {ruta}"
            logger.exception(msg)
            span.record_exception(e)
            span.set_status(trace.StatusCode.ERROR)
            raise



def obtener_att_fecha(fecha):
    with tracer.start_as_current_span("obtener_att_fecha") as span:
        try:
            split = fecha.split('/')
            dia, mes, año = (int(x) for x in split)
            d = date(año, mes, dia)
            diaSemana = cambio_diaSemana[d.strftime("%A").lower()]
            id = split[2] + split[1] + split[0]
            
            logger.info(f"Extracted date attributes: id={id}, dia={dia}, mes={mes}, año={año}, diaSemana={diaSemana}")
            span.set_attributes({
                "id_fecha": id,
                "dia": dia,
                "mes": mes,
                "año": año,
                "diaSemana": diaSemana
            })
            return id, dia, mes, año, diaSemana
        except Exception as e:
            logger.exception(f"Error extracting date attributes from {fecha}")
            span.record_exception(e)
            span.set_status(trace.StatusCode.ERROR)
            raise

def obtener_campo(root, campo):
    with tracer.start_as_current_span("obtener_campo") as span:
        try:
            out = root.find('./articulo/' + campo)
            if out is not None:
                out = out.text
            return out if out != '-' else None
        except Exception as e:
            logger.exception(f"Error retrieving field '{campo}' from XML.")
            span.record_exception(e)
            span.set_status(trace.StatusCode.ERROR)
            raise


# Devuelve una lista con los puestos
def obtener_puestos(root):
    with tracer.start_as_current_span("obtener_puestos") as span:
        try:
            out = []
            aux = root.find('./articulo/puestos')
            if aux is None:
                logger.warning("No se encontraron puestos en el XML.")
                return out
            
            for item in aux.iterfind('./puesto'):
                out.append(item.text if item.text != '-' else None)
            
            logger.info(f"Found {len(out)} puestos.")
            span.set_attribute("puestos_count", len(out))
            return out
        except Exception as e:
            logger.exception("Error obteniendo puestos.")
            span.record_exception(e)
            span.set_status(trace.StatusCode.ERROR)
            raise

# Almacena los datos del fichero XML cuya raíz se ha pasado como parámetro.
def almacenar(root, root_auxiliar, conn, filename):
    with tracer.start_as_current_span("almacenar") as span:
        try:
            num_min_campos = int(root_auxiliar.find('./num_min_campos').text)
            logger.info(f"Minimum number of fields required: {num_min_campos}")

            cursor = conn.cursor()

            # TABLA FECHA
            textual_pub = obtener_campo(root, 'fecha_publicacion')
            textual_disp = obtener_campo(root, 'fecha_disposicion')
            textual_ini = obtener_campo(root, 'fecha_inicio_presentacion')
            textual_fin = obtener_campo(root, 'fecha_fin_presentacion')

            # TABLA PUESTO
            denominaciones = obtener_puestos(root)
            cuerpo = obtener_campo(root, 'cuerpo')
            escala = obtener_campo(root, 'escala')
            subescala = obtener_campo(root, 'subescala')

            # TABLA CONVOCATORIA
            fuente_datos = obtener_campo(root, 'fuente_datos')
            organo_convocante = obtener_campo(root, 'organo_convocante')
            enlace_convocatoria = obtener_campo(root, 'enlace_convocatoria')
            titulo = obtener_campo(root, 'titulo')
            uri_eli = obtener_campo(root, 'uri_eli')
            rango = obtener_campo(root, 'rango')
            id_orden = obtener_campo(root, 'id_orden')
            grupo = obtener_campo(root, 'grupo')
            tipo_convocatoria = obtener_campo(root, 'tipo_convocatoria')
            datos_contacto = obtener_campo(root, 'datos_contacto')
            plazo = obtener_campo(root, 'plazo')
            num_plazas = obtener_campo(root, 'num_plazas')

            # TABLA OFERTA
            estado = 'Abierta'
            enlace_cierre = None
            
            # Solo guardar en la Base de datos si en el fichero de info se ha encontrado un nº mínimo de campos.
            campos_detectados = num_campos_detectados([
                textual_pub, textual_disp, textual_ini, textual_fin, denominaciones,
                cuerpo, escala, subescala, fuente_datos, organo_convocante,
                enlace_convocatoria, titulo, uri_eli, rango, id_orden, grupo,
                tipo_convocatoria, datos_contacto, plazo, num_plazas
            ])
            span.set_attribute("campos_detectados", campos_detectados)
            
            no_insertar = False
            if campos_detectados >= num_min_campos:
                # Inserción en la base de datos.
                id_fecha_pub = insertar_fecha(cursor, textual_pub)
                id_fecha_disp = insertar_fecha(cursor, textual_disp)
                id_fecha_ini = insertar_fecha(cursor, textual_ini)
                id_fecha_fin = insertar_fecha(cursor, textual_fin)

                if denominaciones is None or not denominaciones:
                    if escala is not None:
                        denominaciones = [escala]
                    elif cuerpo is not None:
                        denominaciones = [cuerpo]
                    elif subescala is not None:
                        denominaciones = [subescala]
                    elif grupo is not None:
                        denominaciones = [grupo]
                    else:
                        denominaciones = ['NO INDICADO']
                elif denominaciones[0] is None:
                    if escala is not None:
                        denominaciones = [escala]
                    elif cuerpo is not None:
                        denominaciones = [cuerpo]
                    elif subescala is not None:
                        denominaciones = [subescala]
                    elif grupo is not None:
                        denominaciones = ['NO INDICADO']
                    else:
                        no_insertar = True

                if not no_insertar:
                    ids_puestos = insertar_puestos(
                        cursor, denominaciones, cuerpo, escala, subescala)

                    id_convocatoria = insertar_convocatoria(
                        cursor, organo_convocante, titulo, uri_eli, enlace_convocatoria,
                        grupo, plazo, rango, id_orden, fuente_datos, tipo_convocatoria,
                        datos_contacto, num_plazas, id_fecha_pub, id_fecha_disp,
                        id_fecha_ini, id_fecha_fin)

                    insertar_oferta(cursor, estado, enlace_cierre, id_convocatoria,
                                    ids_puestos)

                else:
                    logger.warning("No se insertaron puestos debido a falta de denominaciones.")
                    
                    
            else:
                no_insertar = True
            
            if no_insertar:
                nombre_articulo = filename.split('.')[0]
                fecha_publicacion = datetime.strptime(textual_pub, '%d/%m/%Y')

                with open('articulos_no_insertados.log', 'a') as file:
                    file.write(f'{nombre_articulo}\n')

                try:
                    sql = ('INSERT INTO articulos_no_insertados(nombre, fecha, enlace, '
                           'num_campos_detectados) '
                           'VALUES(%s, %s, %s, %s);')
                    cursor.execute(sql, (nombre_articulo, fecha_publicacion,
                                         enlace_convocatoria, campos_detectados))
                except psycopg2.DatabaseError as e:
                    logger.exception("Error inserting into articulos_no_insertados.")
                    span.record_exception(e)
                    span.set_status(trace.StatusCode.ERROR)
                    raise

            cursor.close()
        except Exception as e:
            logger.exception("Error in almacenar function.")
            span.record_exception(e)
            span.set_status(trace.StatusCode.ERROR)
            raise
    

# Almacenar lo del fichero indicado según el caso indicado.
def almacenar_pruebas_aceptacion(caso):
    with tracer.start_as_current_span("almacenar_pruebas_aceptacion") as span:
        ruta_auxiliar = Path(r'C:\AragonOpenData\aragon-opendata\tools\ficheros_configuracion\auxiliar.xml')
        ruta_casos = Path(r'C:\Users\opotrony\Desktop\Artículos de casos de prueba')

        cwd = ruta_casos / ('Caso_' + caso)
        for file in os.listdir(cwd):
            if '.xml' in file and 'copia' not in file:
                ruta_info = cwd / file

                try:
                    conn = psycopg2.connect(dbname='empleo_publico_aragon', user='postgres', password='Postgres1',
                                            host='localhost', port=5432)
                    almacenar(obtener_root_fichero(ruta_info), obtener_root_fichero(ruta_auxiliar), conn, file.split('.')[0])
                    conn.commit()
                    conn.close()
                    span.set_attribute("file.processed", file)
                except Exception as e:
                    logger.exception("Error processing prueba de aceptación file.")
                    span.record_exception(e)
                    span.set_status(trace.StatusCode.ERROR)
                    raise

# Almacena los datos de todos los ficheros de info del directorio y día pasados.
def almacenar_todos(dia, directorio_base, ruta_auxiliar, conn):
    with tracer.start_as_current_span(f"Almacenamiento {dia}") as span:
        for filename in os.listdir(directorio_base / dia / 'apertura' / 'info'):
            ruta_info = directorio_base / dia / 'apertura' / 'info' / filename
            try:
                almacenar(obtener_root_fichero(ruta_info),
                          obtener_root_fichero(ruta_auxiliar), conn, filename)
                conn.commit()
                span.set_attribute("file.processed", filename)
            except Exception as e:
                logger.exception("Error processing file in almacenar_todos.")
                span.record_exception(e)
                span.set_status(trace.StatusCode.ERROR)
                continue


# Devuelve True si hay convocatorias del día indicado en la base de datos.
def hay_convocatorias(conn, dia):
    with tracer.start_as_current_span("Comprobar si hay convocatorias") as span:
        cursor = conn.cursor()
        try:
            query = ('SELECT id '
                     'FROM convocatoria '
                     'WHERE id_fecha_publicacion = %s;')
            cursor.execute(query, (dia,))
            result = cursor.fetchone()
            span.set_attribute("convocatorias.exists", result is not None)
            return result is not None
        except Exception as e:
            logger.exception("Error checking convocatorias.")
            span.record_exception(e)
            span.set_status(trace.StatusCode.ERROR)
            raise
        finally:
            cursor.close()


# Borra las ofertas y convocatorias del día indicado y almacena las del directorio indicado, de forma atómica.
def borrar_y_almacenar(dia, directorio_base, ruta_auxiliar, conn):
    with tracer.start_as_current_span(f"Borrar convocatorias {dia} ") as span:
        try:
            cursor = conn.cursor()

            # BORRAR OFERTAS
            query = ('DELETE '
                     'FROM oferta '
                     'WHERE id_convocatoria IN '
                     '(SELECT id '
                     'FROM convocatoria '
                     'WHERE id_fecha_publicacion = %s);')
            cursor.execute(query, (dia,))
            logger.info(f"Deleted offers for date {dia}.")
            
            # BORRAR CONVOCATORIAS
            query = ('DELETE '
                     'FROM convocatoria '
                     'WHERE id_fecha_publicacion = %s;')
            cursor.execute(query, (dia,))
            logger.info(f"Deleted convocations for date {dia}.")
            
            # Almacenar ofertas y convocatorias nuevas
            for filename in os.listdir(directorio_base / dia / 'apertura' / 'info'):
                ruta_info = directorio_base / dia / 'apertura' / 'info' / filename
                try:
                    almacenar(obtener_root_fichero(ruta_info),
                              obtener_root_fichero(ruta_auxiliar), conn, filename)
                except Exception as e:
                    fichero_log = Path(__file__).absolute().parent.parent / 'articulos_erroneos.log'
                    with open(fichero_log, 'a') as file:
                        file.write(f'{filename.split(".")[0]} - SEGUNDO ALMACENAMIENTO\n')
                    logger.error(f"Failed to store {filename} on second attempt.")
                    span.record_exception(e)
                    span.set_status(trace.StatusCode.ERROR)
                    raise

            conn.commit()
            cursor.close()
            span.set_status(trace.StatusCode.OK)
        except Exception as e:
            logger.exception("Error in borrar_y_almacenar function.")
            span.record_exception(e)
            span.set_status(trace.StatusCode.ERROR)
            raise

def main():
    
    if len(sys.argv) == 3:
        caso = sys.argv[1]
        logger.info(f"Starting prueba de aceptación for caso: {caso}")
        try:
            almacenar_pruebas_aceptacion(caso)
        except Exception as e:
            logger.error(f"Error during prueba de aceptación for caso: {caso}", exc_info=e)
    elif len(sys.argv) != 5:
        logger.error(f'Número de parámetros: {len(sys.argv)}')
        logger.error('Número de parámetros incorrecto.')

    else:
        dia = sys.argv[1]
        directorio_base = Path(sys.argv[2])
        ruta_auxiliar = Path(sys.argv[3])
        
        env_path = os.path.join(os.path.dirname(__file__), '..', '.env')
        load_dotenv(dotenv_path=env_path)

        PSQL_HOST = os.getenv("BACK_HOST")
        PSQL_USER = os.getenv("DB_EMPLEO_USER")
        PSQL_PASS = os.getenv("DB_EMPLEO_PASS")
        PSQL_DB = os.getenv("DB_EMPLEO_NAME")
        PSQL_PORT = os.getenv("DB_EMPLEO_PORT")
        dag_id = sys.argv[-1]

        with tracer.start_as_current_span("Almacenamiento") as span:
            try:
                conn = psycopg2.connect(
                    dbname=PSQL_DB, user=PSQL_USER, password=PSQL_PASS, host=PSQL_HOST,
                    port=PSQL_PORT)
                
                logger.info(f"Connection to PostgreSQL established with database: {PSQL_DB}")
                
                if hay_convocatorias(conn, dia):
                    logger.info(f"Existing convocatorias found for date: {dia}. Starting delete and store process.")
                    borrar_y_almacenar(dia, directorio_base, ruta_auxiliar, conn)
                else:
                    logger.info(f"No existing convocatorias found for date: {dia}. Starting store all process.")
                    almacenar_todos(dia, directorio_base, ruta_auxiliar, conn)
                
                conn.close()
                logger.info("PostgreSQL connection closed.")
            except psycopg2.DatabaseError as e:
                logger.error("Database error occurred.", exc_info=e)
            except Exception as e:
                logger.error("An unexpected error occurred.", exc_info=e)

if __name__ == "__main__":
	main()
