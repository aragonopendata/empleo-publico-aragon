import sys
import os
import logging
import pathlib
import re

from xml.etree import ElementTree as ET
import psycopg2
import spacy

import datetime
from time import strptime
import locale

# [TRACER Y LOGGER]
from opentelemetry import trace

sys.path.append(os.path.abspath('/app/ingesta-BO'))
from tracer.tracer_configurator import TracerConfigurator
from logger.logger_configurator import LoggerConfigurator

dag_id = sys.argv[-1]

tracer_configurator = TracerConfigurator(dag_id=dag_id)
tracer = tracer_configurator.get_tracer()

logger_configurator = LoggerConfigurator(name='Cierres', dag_id=dag_id)
logger = logger_configurator.get_logger()

locale.setlocale(locale.LC_ALL, 'es_ES.UTF-8')

def cambio_mes(mes_str, ruta_fichero_aux):
    """Convierte el nombre del mes en español a su representación numérica."""
    try:
        with open(Path(ruta_fichero_aux), 'rb') as file:
            tree_aux = ET.parse(file)
            root_aux = tree_aux.getroot()
        mes_num = root_aux.find('./correspondencias_meses/' + mes_str.lower()).text
        logger.info(f'Conversión de mes "{mes_str}" a número: {mes_num}')
        return mes_num
    except Exception as e:
        logger.exception(f"Error al convertir mes '{mes_str}' usando {ruta_fichero_aux}: {e}")
        raise

def len_puesto(tupla):
		return (len(tupla[1]), tupla)

def obtener_puestos(root):
    """Devuelve una lista con los puestos extraídos del XML."""
    out = []
    aux = root.find('./articulo/puestos')
    
    if aux is not None:
        for item in aux.iterfind('./puesto'):
            puesto_text = item.text.lower() if item.text != '-' else None
            out.append(puesto_text)
            logger.debug(f'Puesto encontrado: {puesto_text}')
    else:
        logger.debug('No se encontraron puestos en el XML.')
    
    return out

def obtener_fecha_disposicion_oferta(txt_filepath, root_info, ruta_fichero_regex, ruta_fichero_aux):
    """Identifica la fecha de disposición de la oferta a partir del texto y reglas proporcionadas."""
    with tracer.start_as_current_span("obtener_fecha_disposicion_oferta") as span:
        try:
            # Recuperar fichero con las expresiones regulares
            with open(ruta_fichero_regex, 'rb') as file:
                tree_re = ET.parse(file)
                root_re = tree_re.getroot()
            regex_fecha_disposicion = root_re.find('./reglas/cierre/texto_fecha_disposicion').text
            span.set_attribute("regex_fecha_disposicion", regex_fecha_disposicion)
            logger.info(f'Regex para fecha de disposición: {regex_fecha_disposicion}')
            
            # Leer texto
            with open(txt_filepath, 'r', encoding='utf-8') as file:
                texto = file.read()
            span.set_attribute("txt_filepath", txt_filepath)
            logger.info(f'Contenido del archivo de texto leído: {txt_filepath}')
            
            nlp = spacy.load("es_core_news_md")
            doc_texto = nlp(texto)
            
            # Encontrar match
            matches = []
            for match in re.finditer(regex_fecha_disposicion, doc_texto.text):
                start, end = match.span()
                span_text = doc_texto.char_span(start, end)
                if span_text is not None:
                    matches.append(span_text.text)
            logger.debug(f'Matches encontrados para la fecha de disposición: {matches}')
            
            if not matches or len(matches) <= 1:
                logger.error(f"No se encontraron suficientes coincidencias para la fecha de disposición en {txt_filepath}")
                return -1
            
            match = matches[1]
            logger.info(f'Match seleccionado para fecha de disposición: {match}')
            
            # Post procesar la cadena encontrada
            split_match = match.split(' ')
            if split_match[0] == 'Resolución':
                ultimo = split_match[-1]
                if ultimo.isnumeric():
                    anyo = ultimo.zfill(4)
                    mes = split_match[-3]
                    dia = split_match[-5].zfill(2)
                else:
                    mes = ultimo.zfill(2)
                    dia = split_match[-3].zfill(2)
                    fpub = root_info.find('./articulo/fecha_publicacion').text
                    anyo = int(fpub.split('/')[-1])
                    fpub_str = strptime(fpub, "%d/%m/%Y")
                    d_p = datetime(fpub_str[0], fpub_str[1], fpub_str[2])

                    # No se sabe el año, por lo que se pondrá el pasado más cercano a la fecha de publicación
                    anyo_descubierto = False
                    while not anyo_descubierto and anyo > 1900:
                        fdisp_str = strptime(f"{dia}/{mes}/{anyo}", "%d/%B/%Y")
                        d_d = datetime(fdisp_str[0], fdisp_str[1], fdisp_str[2])
                        
                        # Si con este año la fecha de disposición es anterior a la fecha de publicación -> Se elige este año
                        if (d_d - d_p).days < 0:
                            anyo_descubierto = True
                            anyo = str(anyo)
                        else:
                            anyo -= 1
            elif split_match[0] == 'Orden':
                anyo = split_match[1].split('/')[2].zfill(4)
                dia = split_match[3].zfill(2)
                mes = split_match[5]
            else:
                raise Exception("Formato de fecha no reconocido.")
            
            if mes.isnumeric():
                mes = mes.zfill(2)
            else:
                mes = cambio_mes(mes, ruta_fichero_aux)
            
            fecha_disposicion = f'{dia}/{mes}/{anyo}'
            span.set_attribute("fecha_disposicion", fecha_disposicion)
            logger.info(f'Fecha de disposición identificada: {fecha_disposicion}')
            return fecha_disposicion
        
        except Exception as e:
            logger.exception(f"Error al obtener fecha de disposición: {e}")
            raise

def comprobar_cierre(txt_filepath, info_filepath, ruta_fichero_regex, ruta_fichero_aux, conn, cursor):
    with tracer.start_as_current_span("Comprobar Cierre") as span:
        span.set_attribute("text_filepath", str(txt_filepath))
        span.set_attribute("info_filepath", str(info_filepath))
        span.set_attribute("regex_file", str(ruta_fichero_regex))
        span.set_attribute("aux_file", str(ruta_fichero_aux))
        
        try:
            with open(info_filepath, 'rb') as file:
                tree_info = ET.parse(file)
                root_info = tree_info.getroot()
                logger.info(f"Archivo de información leído correctamente: {info_filepath}")
        except Exception as e:
            msg = f"Failed: Open {info_filepath}"
            logger.exception(msg)
            span.record_exception(e)
            span.set_status(trace.status.StatusCode.ERROR)
            return None

        tipo_boletin = root_info.find('./articulo/fuente_datos').text
        organo = root_info.find('./articulo/organo_convocante').text
        enlace_cierre = root_info.find('./articulo/enlace_convocatoria').text
        titulo = root_info.find('./articulo/titulo').text
        terminos_vaciar = ['desiertas', 'desierta', 'dejar sin efecto']
        
        fecha_disposicion = obtener_fecha_disposicion_oferta(txt_filepath, root_info, ruta_fichero_regex, ruta_fichero_aux)
        span.set_attribute("fecha_disposicion", fecha_disposicion)

        if fecha_disposicion == -1:
            logger.warning(f"No se encontró una fecha de disposición válida en el archivo: {txt_filepath}")
            return

        id_fecha_disposicion = fecha_disposicion.split('/')
        id_fecha_disposicion.reverse()
        id_fecha_disposicion = ''.join(id_fecha_disposicion)

        if any([termino in titulo for termino in terminos_vaciar]):
            query = ('SELECT c.id '
                     'FROM convocatoria c, oferta o '
                     'WHERE c.fuente = %s '
                        'AND c.organo_convocante = %s '
                        'AND c.id_fecha_disposicion = %s '
                        'AND c.id = o.id_convocatoria '
                        'AND o.estado = %s;')
            cursor.execute(query, (tipo_boletin, organo, id_fecha_disposicion, 'Abierta'))
            
            id_convocatoria = cursor.fetchone()
            if id_convocatoria is None:
                logger.info(f"No se encontraron convocatorias abiertas para el cierre en el archivo: {txt_filepath}")
                return

            sql = ('UPDATE oferta '
                   'SET estado = %s, enlace_cierre = %s '
                   'WHERE id_convocatoria = %s;')
            cursor.execute(sql, ('Cerrada', enlace_cierre, id_convocatoria[0]))
            conn.commit()
            logger.info(f"Convocatoria con ID {id_convocatoria[0]} cerrada.")
            return
        
        query = ('SELECT oo.id, p.denominacion '
                 'FROM puesto p '
                 'INNER JOIN ( '
                    'SELECT o.id, o.id_puesto '
                    'FROM oferta o '
                    'INNER JOIN ( '
                        'SELECT c.id '
                        'FROM convocatoria c '
                        'WHERE c.fuente = %s '
                            'AND c.organo_convocante = %s '
                            'AND c.id_fecha_disposicion = %s '
                    ') AS cc '
                    'ON o.id_convocatoria = cc.id '
                    'WHERE o.estado = %s '
                 ') AS oo '
                 'ON p.id = oo.id_puesto;')
        cursor.execute(query, (tipo_boletin, organo, id_fecha_disposicion, 'Abierta'))
        ofertas = cursor.fetchall()
        span.set_attribute("ofertas_encontradas", len(ofertas))

        ofertas.sort(key=len_puesto, reverse=True)

        cerrar_oferta = [False] * len(ofertas)

        lista_puestos_tablas = obtener_puestos(root_info)

        try:
            with open(txt_filepath, 'r', encoding='utf-8') as file:
                texto = file.read()
                logger.info(f"Texto leído correctamente del archivo: {txt_filepath}")
        except Exception as e:
            msg = f"Failed: Read {txt_filepath}"
            logger.exception(msg)
            span.record_exception(e)
            span.set_status(trace.status.StatusCode.ERROR)
            return None

        for i, tupla in enumerate(ofertas):
            puesto = tupla[1].lower()

            if puesto in lista_puestos_tablas:
                cerrar_oferta[i] = True
                lista_puestos_tablas.remove(puesto)
                logger.info(f"Puesto '{puesto}' encontrado en tabla y marcado para cierre.")
            elif puesto in texto.lower():
                cerrar_oferta[i] = True
                texto = texto.replace(puesto, '', 1)
                logger.info(f"Puesto '{puesto}' encontrado en texto y marcado para cierre.")

        existe_cierre = False
        for i, cerrar in enumerate(cerrar_oferta):
            if cerrar:
                id_oferta = ofertas[i][0]
                sql = ('UPDATE oferta '
                       'SET estado = %s, enlace_cierre = %s '
                       'WHERE id = %s;')
                cursor.execute(sql, ('Cerrada', enlace_cierre, str(id_oferta)))
                existe_cierre = True
                logger.info(f"Oferta con ID {id_oferta} cerrada.")

        conn.commit()

        if not existe_cierre:
            with open('articulos_sin_cierres.log', 'a') as file:
                file.write(f'{txt_filepath.parts[-1].split(".")[0]}\n')
                logger.warning(f"No se cerraron ofertas para el archivo: {txt_filepath}")



# Comprueba cierres del caso indicado según el caso indicado.
def comprobar_cierres_pruebas_aceptacion(caso):
    with tracer.start_as_current_span("Comprobar Cierres Pruebas Aceptacion") as span:
        span.set_attribute("caso", caso)
        ruta_auxiliar = pathlib.Path('path/to/auxiliar.xml')
        ruta_regex = pathlib.Path('path/to/regex.xml')
        ruta_casos = pathlib.Path('path/to/casos')
        
        cwd = ruta_casos / ('Caso_' + caso)
        for file in os.listdir(cwd):
            if '.xml' in file and 'copia' not in file:
                ruta_info = cwd / file
            elif '.txt' in file:
                ruta_txt = cwd / file
        conn = psycopg2.connect(dbname='empleo_publico_aragon', user='postgres', password='Postgres1', host='localhost', port=5432)
        cursor = conn.cursor()
        comprobar_cierre(ruta_txt, ruta_info, ruta_regex, ruta_auxiliar, conn, cursor)
        conn.close()

def comprobar_cierres_directorio(directorio_base, dia, ruta_fichero_regex, ruta_fichero_aux, conn):
    with tracer.start_as_current_span("Comprobar Cierres Directorio") as span:
        span.set_attribute("directorio_base", str(directorio_base))
        span.set_attribute("dia", dia)
        cursor = conn.cursor()
        for filename in os.listdir(directorio_base / dia / 'cierre' / 'txt'):
            if '_legible' not in filename:
                ruta_texto = directorio_base / dia / 'cierre' / 'txt' / filename
                ruta_info = directorio_base / dia / 'cierre' / 'info' / (filename.replace('.txt', '.xml'))

                try:
                    comprobar_cierre(ruta_texto, ruta_info, ruta_fichero_regex, ruta_fichero_aux, conn, cursor)
                except Exception as e:
                    fichero_log = pathlib.Path(__file__).absolute().parent.parent / 'articulos_erroneos.log'
                    with open(fichero_log, 'a') as file:
                        file.write(f'{filename.split(".")[0]} - CIERRES\n')
                    logger.exception(f"Error processing file {filename}")
                    span.record_exception(e)
        cursor.close()


def main():
    with tracer.start_as_current_span("Main Function Execution") as span:
        if len(sys.argv) == 2:
            comprobar_cierres_pruebas_aceptacion(sys.argv[1])
        elif len(sys.argv) != 10:
            logger.error('Numero de parametros incorrecto.')
            return
        else:
            directorio_base = pathlib.Path(sys.argv[1])
            dia = sys.argv[2]
            ruta_fichero_regex = pathlib.Path(sys.argv[3])
            ruta_fichero_aux = pathlib.Path(sys.argv[4])
            
            PSQL_HOST = os.getenv("BACK_HOST")
            PSQL_USER = os.getenv("DB_EMPLEO_USER")
            PSQL_PASS = os.getenv("DB_EMPLEO_PASS")
            PSQL_DB = os.getenv("DB_EMPLEO_NAME")
            PSQL_PORT = os.getenv("DB_EMPLEO_PORT")

            conn = psycopg2.connect(
                dbname=PSQL_DB, user=PSQL_USER, password=PSQL_PASS, host=PSQL_HOST,
                port=PSQL_PORT)

            comprobar_cierres_directorio(directorio_base, dia, ruta_fichero_regex, ruta_fichero_aux, conn)
            
            conn.close()


if __name__ == "__main__":
	main()