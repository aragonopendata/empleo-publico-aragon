# [SYSTEM]
import sys
import os
import logging
import re

from xml.etree import ElementTree as ET
from pathlib import Path
from datetime import date, timedelta, datetime
from time import strptime
import spacy

from workalendar.europe import Spain
from spa2num.converter import to_number
import locale

# [CLASES]
from extraccion_reglas import ExtraccionReglas
from extraccion_ner import ExtraccionNER
from extraccion_tablas import ExtraccionTablas

# [TRACER Y LOGGER]
from opentelemetry import trace
sys.path.append(os.path.abspath('/opt/airflow/ingesta-BO'))

from tracer.tracer_configurator import TracerConfigurator
from logger.logger_configurator import LoggerConfigurator

dag_id = sys.argv[-1]

tracer_configurator = TracerConfigurator(service_name=f'Extracción Task - {dag_id}', dag_id=dag_id)
tracer = tracer_configurator.get_tracer()

logger_configurator = LoggerConfigurator(name='Extracción', dag_id=dag_id)
logger = logger_configurator.get_logger()

# Iniciar calendario español
cal = Spain()
locale.setlocale(locale.LC_ALL, 'es_ES.UTF-8')

extraccion_reglas = ExtraccionReglas(tracer, logger)
extraccion_ner = ExtraccionNER(tracer, logger)
extraccion_tablas = ExtraccionTablas(tracer, logger)

# Devuelve la raíz del fichero indicado
def obtener_root_fichero(ruta):
    with tracer.start_as_current_span("Obtener raíz del fichero") as span:
        span.set_attribute("ruta", str(ruta))
        try:
            with open(ruta, 'rb') as file:
                tree = ET.parse(file)
                root = tree.getroot()
                logger.info(f"Fichero {ruta} cargado correctamente.")
        except Exception as e:
            msg = f"Failed: Read {ruta}"
            logger.exception(msg)
            span.record_exception(e)
            span.set_status(trace.StatusCode.ERROR)
            raise
        return root

# Devuelve el elemento del metadato indicado de la raíz indicada.
# Para comprobar si existe, mirar si no es None. Para el texto, .text
def obtener_metadato(root, metadato):
	return root.find('./articulo/' + metadato)

# Devuelve los matches del documento con la regex pasada.
def encontrar_matches(regex, documento, ignore_case=True):
    matches = []
    with tracer.start_as_current_span("Encontrar coincidencias") as span:
        span.set_attribute("regex", regex)
        span.set_attribute("ignore_case", ignore_case)
        if ignore_case:
            iter = re.finditer(regex, documento.text, flags=re.IGNORECASE)
        else:
            iter = re.finditer(regex, documento.text)
        
        for match in iter:
            start, end = match.span()
            span_ = documento.char_span(start, end)
            if span_ is not None:
                matches.append(span_.text)
        span.set_attribute("matches_encontrados", len(matches))
    return matches

# Devuelve una lista de los grupos encontrados en la lista sin limpiar pasada.
def limpiar_grupos(lista_grupos):
    regex_posibles_grupos = '(A1|A2|A|B|C1|C2|C|E|GP1|GP2|GP3|GP4|GP5)'
    regex_codigos = '[ABCE]{1}[0-9]{3}'
    out = []
    nlp = spacy.load("es_core_news_md")
    with tracer.start_as_current_span("Limpiar grupos") as span:
        for texto in lista_grupos:
            doc = nlp(texto)
            encontrado = encontrar_matches(regex_codigos, doc, False)
            if encontrado:
                out.append(texto[0])
            else:
                encontrado = encontrar_matches(regex_posibles_grupos, doc, False)
                if not encontrado:
                    break
                elif len(encontrado) == 1:
                    out.append(encontrado[0])
                else:
                    for e in encontrado: out.append(e)
        span.set_attribute("grupos_limpios", len(out))
    return out

# Devuelve la concatenación de todos los grupos únicos encontrados.
def juntar_grupos(lista_1, lista_2):
    with tracer.start_as_current_span("Juntar grupos") as span:
        if lista_1 or lista_2:
            result = '/'.join(sorted(list(set(lista_1 + lista_2))))
        else:
            result = '-'
        span.set_attribute("grupos_juntados", result)
    return result

# Devuelve la lista de términos del tipo tipo aparecidos en el fichero de regex
def obtener_terminos_tipo(root, tipo):
    terminos = []
    with tracer.start_as_current_span("Obtener términos tipo") as span:
        span.set_attribute("tipo", tipo)
        for t in root.find('./terminos_tipo/' + tipo).findall('./termino'):
            terminos.append(t.text)
        span.set_attribute("terminos_encontrados", len(terminos))
    return terminos

def obtener_tipo(root_regex, tit_reg, tex_reg, tex_ner):
    with tracer.start_as_current_span("Obtener tipo convocatoria") as span:
        terminos_ambas = obtener_terminos_tipo(root_regex, 'libre_e_interna')
        terminos_libre = obtener_terminos_tipo(root_regex, 'libre')
        terminos_interna = obtener_terminos_tipo(root_regex, 'interna')

        tr_libre_reglas, tr_interna_reglas = 0, 0
        for tr in tit_reg:
            if tr in terminos_ambas:
                span.set_attribute("resultado", "Libre+Interna")
                return 'Libre+Interna'
            elif tr in terminos_libre:
                tr_libre_reglas += 1
            elif tr in terminos_interna:
                tr_interna_reglas += 1
        if tr_libre_reglas and tr_interna_reglas:
            span.set_attribute("resultado", "Libre+Interna")
            return 'Libre+Interna'
        elif tr_libre_reglas:
            span.set_attribute("resultado", "Libre")
            return 'Libre'
        elif tr_interna_reglas:
            span.set_attribute("resultado", "Interna")
            return 'Interna'

        tr_libre_reglas, tr_interna_reglas = 0, 0
        for tr in tex_reg:
            if tr in terminos_ambas:
                span.set_attribute("resultado", "Libre+Interna")
                return 'Libre+Interna'
            elif tr in terminos_libre:
                tr_libre_reglas += 1
            elif tr in terminos_interna:
                tr_interna_reglas += 1
        if tr_libre_reglas and tr_interna_reglas:
            span.set_attribute("resultado", "Libre+Interna")
            return 'Libre+Interna'

        tr_libre_ner, tr_interna_ner = 0, 0
        for tr in tex_ner:
            if tr in terminos_ambas:
                span.set_attribute("resultado", "Libre+Interna")
                return 'Libre+Interna'
            elif tr in terminos_libre:
                tr_libre_ner += 1
            elif tr in terminos_interna:
                tr_interna_ner += 1
        if tr_libre_ner and tr_interna_ner:
            span.set_attribute("resultado", "Libre+Interna")
            return 'Libre+Interna'

        if tr_libre_reglas:
            span.set_attribute("resultado", "Libre")
            return 'Libre'
        elif tr_interna_reglas:
            span.set_attribute("resultado", "Interna")
            return 'Interna'
        elif tr_libre_ner:
            span.set_attribute("resultado", "Libre")
            return 'Libre'
        elif tr_interna_ner:
            span.set_attribute("resultado", "Interna")
            return 'Interna'

        span.set_attribute("resultado", "Libre")
        return 'Libre'

# Devuelve un dato de contacto de los pasados (websites e emails).
def obtener_datos_contacto(web_reg, em_reg, web_ner, em_ner):
    with tracer.start_as_current_span("Obtener datos de contacto") as span:
        span.set_attribute("web_reg", bool(web_reg))
        span.set_attribute("em_reg", bool(em_reg))
        span.set_attribute("web_ner", bool(web_ner))
        span.set_attribute("em_ner", bool(em_ner))

        if web_reg:
            resultado = web_reg[0]
        elif em_reg:
            resultado = em_reg[0]
        elif web_ner:
            resultado = web_ner[0]
        elif em_ner:
            resultado = em_ner[0]
        else:
            resultado = '-'

        span.set_attribute("resultado", resultado)
        return resultado

# Devuelve el primer elemento de la lista primera.
# Si no lo hay, el primero de la segunda. Sino, '-'.
def primero_por_preferencia(lista1, lista2):
    with tracer.start_as_current_span("Seleccionar primer elemento por preferencia") as span:
        span.set_attribute("lista1_length", len(lista1))
        span.set_attribute("lista2_length", len(lista2))

        if lista1:
            resultado = lista1[0]
        elif lista2:
            resultado = lista2[0]
        else:
            resultado = '-'

        span.set_attribute("resultado", resultado)
        return resultado

# Devuelve las fechas de inicio y fin de presentación de solicitudes,
# detectadas a partir del plazo y la fecha de publicación.
def obtener_fechas_presentacion(root_regex, plazo, fpub):
    with tracer.start_as_current_span("Obtener fechas de presentación de solicitudes") as span:

        if plazo == '-': return '-', '-'
        regex_tipo_dias = root_regex.find('./reglas/apertura/plazo/tipo_dias').text
        regex_contexto_1 = root_regex.find('./reglas/apertura/plazo/contexto_1').text
        regex_dia_inicio = root_regex.find('./reglas/apertura/plazo/dia_inicio').text

        span.set_attribute("plazo", plazo)
        span.set_attribute("regex_tipo_dias", regex_tipo_dias)
        span.set_attribute("regex_contexto_1", regex_contexto_1)
        span.set_attribute("regex_dia_inicio", regex_dia_inicio)
        
        try:
            match = re.search(regex_tipo_dias+regex_contexto_1+regex_dia_inicio, plazo)
            dias = to_number(plazo[:match.span()[0]].split('(')[0])
            plazo = plazo[match.span()[0]:]

            match = re.match(regex_tipo_dias, plazo)
            split = plazo[match.span()[0]:match.span()[1]].split(' ')
            if 'mes' in split[1]:
                dias *= 30
            es_habil = len(split) > 2 and ('hábil' in split[2] or 'laborable' in split[2])

            plazo = plazo[match.span()[1]:]
            match = re.match(regex_contexto_1, plazo)
            plazo = plazo[match.span()[1]:]
            match = re.match(regex_dia_inicio, plazo)
            es_dia_siguiente = 'siguiente' in plazo[:match.span()[1]]
        except Exception as e:
            logger.exception("Error al calcular fechas de presentación")
            span.record_exception(e)
            span.set_status(trace.StatusCode.ERROR)
            return '-', '-'

        inicio = date(int(fpub[6:]), int(fpub[3:5]), int(fpub[:2]))
        if es_dia_siguiente:
            inicio += timedelta(days=1)
        fecha_inicio = inicio.strftime('%d/%m/%Y')

        if es_habil:
            fecha_fin = cal.add_working_days(inicio, dias)
        else:
            fecha_fin = inicio + timedelta(days=dias)
        fecha_fin = fecha_fin.strftime('%d/%m/%Y')

        span.set_attribute("fecha_inicio", fecha_inicio)
        span.set_attribute("fecha_fin", fecha_fin)
        return fecha_inicio, fecha_fin

# Devuelve el texto pasado, limpiado y convertido a número
def limpiar_texto_plazas(texto):
    with tracer.start_as_current_span("Limpiar texto de plazas") as span:
        texto = texto.lower()
        if ' y ' in texto:
            split_y = texto.split(' ')
            for i, l in enumerate(split_y):
                if l == 'y':
                    i_y = i
            texto = split_y[i_y-1] + ' y ' + split_y[i_y+1]
            try:
                texto = to_number(texto)
                span.set_attribute("resultado", texto)
                return texto
            except:
                texto = split_y[i_y+1]

        terminos_a_eliminar = ['puestos', 'vacantes', 'plazas', 'puesto', 'vacante', 'plaza', 'número de', ':', 'totales']
        terminos_una_plaza = ['una', 'un', 'del', 'el', 'la']
        for t in terminos_a_eliminar:
            texto = texto.replace(t, '')
        texto = texto.rstrip().lstrip()
        if texto in terminos_una_plaza:
            texto = '1'
        try:
            texto = to_number(texto)
        except:
            texto = '-'
        
        span.set_attribute("resultado", texto)
        return texto


# Devuelve el número de plazas totales, que ha de ser mayor o igual que num_puestos.
def obtener_num_plazas(tit_reg, tex_reg, tex_ner, num_puestos):
    with tracer.start_as_current_span("Obtener número de plazas") as span:
        span.set_attribute("tit_reg_length", len(tit_reg))
        span.set_attribute("tex_reg_length", len(tex_reg))
        span.set_attribute("tex_ner_length", len(tex_ner))

        if tit_reg:
            for p in tit_reg:
                if 'bolsa de trabajo' in p.lower():
                    span.set_attribute("resultado", '-')
                    return '-'
                p = limpiar_texto_plazas(p)
                if p != '-':
                    span.set_attribute("resultado", p)
                    return p

        if len(tex_reg) + len(tex_ner) == 0:
            resultado = num_puestos if num_puestos else '-'
            span.set_attribute("resultado", resultado)
            return resultado

        lista = []
        for t in tex_reg + tex_ner:
            if 'bolsa de trabajo' in t.lower():
                span.set_attribute("resultado", '-')
                return '-'
            t = limpiar_texto_plazas(t)
            if t != '-':
                lista.append(t)

        lista = sorted(list(set(lista)), reverse=True)
        if lista:
            resultado = lista[0] if lista[0] >= num_puestos else num_puestos
        else:
            resultado = num_puestos if num_puestos else '-'
        span.set_attribute("resultado", resultado)
        return resultado

# Devuelve una lista con stopwords en español, leída del fichero ../ficheros_configuracion/stopwords.txt
def leer_stopwords():
    with tracer.start_as_current_span("Leer stopwords") as span:
        out = []
        ruta_fcs = Path(__file__).absolute().parent.parent / 'ficheros_configuracion'
        ruta_fichero_stopwords = ruta_fcs / 'stopwords.txt'
        try:
            with open(ruta_fichero_stopwords, encoding='utf-8') as file:
                for line in file:
                    out.append(line.rstrip('\n'))
            span.set_attribute("stopwords_count", len(out))
        except Exception as e:
            logger.exception("Error al leer stopwords")
            span.record_exception(e)
            span.set_status(trace.StatusCode.ERROR)
        return out

# Quita escalas incorrectas que ha podido coger el ner
def quitar_escalas_incorrectas(lista):
    with tracer.start_as_current_span("Quitar escalas incorrectas") as span:
        cadenas = ['escala a que pertenece', 'escala a la que pertenece', 'escala grupo', 'escala o clase de especialidad',
                   'escala situación administrativa', 'escala por promoción', 'escala de procedencia', 'escala subgrupo',
                   'escala grup/subg', 'escala de funcionario']
        cortas_correctas = ['de', 'y']
        out = []
        for e in lista:
            meter = True
            for cadena in cadenas:
                if cadena in e.lower():
                    meter = False
            for word in e.lower().split(' '):
                if len(word) < 4 and word not in cortas_correctas:
                    meter = False
            if meter:
                out.append(e)
        span.set_attribute("escalas_filtradas", len(out))
        return out

# Quita cuerpos incorrectos que ha podido coger el ner
def quitar_cuerpos_incorrectos(lista):
    with tracer.start_as_current_span("Quitar cuerpos incorrectos") as span:
        cortas_correctas = ['del', 'de', 'la', 'y']
        out = []
        for e in lista:
            meter = True
            for word in e.lower().split(' '):
                if len(word) < 4 and word not in cortas_correctas:
                    meter = False
            if meter:
                out.append(e)
        span.set_attribute("cuerpos_filtrados", len(out))
        return out

# Devuelve texto, sin stopwords al principio ni al final del mismo.
def evitar_extremo_stopword(texto, stopwords):
    with tracer.start_as_current_span("Evitar extremo stopword") as span:
        texto = texto.rstrip(' \t\n.:,(')
        while texto:
            split = texto.split(' ')
            if split[-1] not in stopwords:
                break
            else:
                texto = ' '.join(split[:-1]).rstrip(' \t\n.:,(')
        texto = texto.lstrip(' \t\n.:,)')
        while texto:
            split = texto.split(' ')
            if split[0] not in stopwords:
                break
            else:
                texto = ' '.join(split[1:]).lstrip(' \t\n.:,)')
        span.set_attribute("texto_resultado", texto)
        return texto

# Quitar de lista los elementos que empiezan por una stopword
def quitar_puestos_con_inicio_stopword(lista, stopwords):
    with tracer.start_as_current_span("Quitar puestos con inicio stopword") as span:
        out = []
        for e in lista:
            e = e.lstrip(' \t\n.-')
            if e.split(' ')[0] not in stopwords:
                out.append(e)
        span.set_attribute("puestos_filtrados", len(out))
        return out

# Devuelve el puesto del ner tras quitarle ciertas substrings.
def limpiar_puesto_ner(texto, stopwords):
    with tracer.start_as_current_span("Limpiar puesto NER") as span:
        terminos_a_eliminar = ['especialidad']
        terminos_eliminatorios = ['denominacion', 'denominación']

        for t in terminos_eliminatorios:
            if t in texto:
                span.set_attribute("puesto_eliminado", True)
                return None
        
        for t in terminos_a_eliminar:
            texto = texto.replace(t, '')

        texto = evitar_extremo_stopword(texto, stopwords)
        if texto == '':
            span.set_attribute("puesto_eliminado", True)
            return None
        
        span.set_attribute("puesto_resultado", texto)
        return texto

# Devuelve los puestos del ner pasados como lista, tras quitarles ciertas substrings.
def limpiar_puestos_ner(lista, stopwords):
    with tracer.start_as_current_span("Limpiar puestos NER") as span:
        out = []
        for e in lista:
            e = limpiar_puesto_ner(e, stopwords)
            if e is not None:
                out.append(e)
        span.set_attribute("puestos_limpiados", len(out))
        return out

# Devuelve una lista con los puestos detectados.
# (Los puestos de reglas vienen más o menos limpios, mientras que los del ner no)
def obtener_puestos(p_reg, p_ner, p_tablas, stopwords):
    with tracer.start_as_current_span("Obtener puestos") as span:
        if p_tablas:
            resultado = [p.lower().replace('código de puesto', '').rstrip(' \t\n.:,–-(0123456789)').lstrip(' \t\n.:,)-').capitalize() for p in p_tablas]
            span.set_attribute("resultado", resultado)
            return resultado
        if p_reg:
            resultado = [p_reg[0].lower().rstrip(' \t\n.:,(').lstrip(' \t\n.:,)-').capitalize()]
            span.set_attribute("resultado", resultado)
            return resultado

        p_ner = limpiar_puestos_ner([e.lower() for e in p_ner], stopwords)
        if p_ner:
            resultado = [p_ner[0].capitalize()]
        else:
            resultado = '-'
        span.set_attribute("resultado", resultado)
        return resultado

# Devuelve la fecha de disposición limpia (la primera de la lista, que es no vacía).

def obtener_fecha_disposicion(lista, fpub, root_aux):
    with tracer.start_as_current_span("Obtener fecha de disposición") as span:
        if not lista:
            span.set_attribute("fecha_disposicion", fpub)
            return fpub
        split = [e.strip() for e in lista[0].split('de') if e]
        dia = split[0].zfill(2)
        mes = root_aux.find('./correspondencias_meses/' + split[1].lower()).text
        if len(split) > 2:
            anyo = split[2].zfill(4)
        else:
            anyo = int(fpub.split('/')[-1])
            fpub_str = strptime(fpub, "%d/%m/%Y")
            d_p = datetime(fpub_str[0], fpub_str[1], fpub_str[2])

            anyo_descubierto = False
            while not anyo_descubierto and anyo > 1900:
                fdisp_str = strptime(dia + '/' + split[1].lower() + '/' + str(anyo), "%d/%B/%Y")
                d_d = datetime(fdisp_str[0], fdisp_str[1], fdisp_str[2])

                if (d_d - d_p).days < 0:
                    anyo_descubierto = True
                    anyo = str(anyo)
                else:
                    anyo -= 1

        fecha_disposicion = dia + '/' + mes + '/' + anyo
        span.set_attribute("fecha_disposicion", fecha_disposicion)
        return fecha_disposicion

# Devuelve lista, sin los elementos que no contienen el texto texto.
def limpiar_por_texto(lista, texto):
    with tracer.start_as_current_span("Limpiar por texto") as span:
        out = []
        for e in lista:
            e = e.lower()
            if texto in e:
                out.append(e.strip(' \t\n.,:').capitalize())
        span.set_attribute("elementos_filtrados", len(out))
        return out

# Devuelve el subelemento pasado tras incorporarle el campo indicado con el texto indicado.
def escribir_en_info(SE, campo, texto):
    with tracer.start_as_current_span("Escribir en info") as span:
        campo_ET = ET.SubElement(SE, campo)
        campo_ET.text = texto.replace('\n', ' ')
        span.set_attribute("campo", campo)
        span.set_attribute("valor", campo_ET)
        return SE

# Devuelve el subelemento pasado tras incorporarle los puestos indicados.

def escribir_puestos_en_info(SE, puestos):
    with tracer.start_as_current_span("Escribir puestos en info") as span:
        if puestos == '-':
            resultado = escribir_en_info(SE, 'puestos', puestos)
            span.set_attribute("resultado", "sin puestos")
            return resultado
        puestos = [p.lower().replace('código de puesto', '').rstrip(' \t\n.:,–-(0123456789)').lstrip(' \t\n.:,)-').capitalize() for p in puestos]
        
        puestos_ET = ET.SubElement(SE, 'puestos')
        for puesto in puestos:
            puesto_ET = ET.SubElement(puestos_ET, 'puesto')
            puesto_ET.text = puesto.replace('\n', ' ')
        
        span.set_attribute("puestos", len(puestos))
        return SE

# Añade los puestos leídos de tablas en el fichero pdf pasado al fichero de info

def evaluar_tablas_cierre(dia, ruta_info, ruta_texto, ruta_pdf, ruta_regex, ruta_auxiliar, ruta_modelo_NER):
    with tracer.start_as_current_span("Evaluar tablas cierre") as span:
        root_info = obtener_root_fichero(ruta_info)
        stopwords = leer_stopwords()

        puestos_tablas = quitar_puestos_con_inicio_stopword(extraccion_tablas.obtener_puestos(ruta_pdf, ruta_auxiliar), stopwords)
        ents_reglas = extraccion_reglas.obtener_campos_reglas(dia, ruta_info, ruta_texto, ruta_regex)
        ents_ner = extraccion_ner.obtener_campos_ner(ruta_texto, ruta_modelo_NER)

        puestos = obtener_puestos(
            ents_reglas.get('puesto', []),
            ents_ner.get('puesto', []),
            puestos_tablas,
            stopwords
        )

        articulo = root_info.find('articulo')
        articulo = escribir_puestos_en_info(articulo, puestos)

        with open(ruta_info, 'wb') as file:
            tree_info = ET.ElementTree(root_info)
            tree_info.write(file)
        span.set_attribute("puestos_evaluados", len(puestos))


# Modifica el fichero info pasado con todos los campos encontrados agregados.
def evaluar_articulo(dia, ruta_info, ruta_texto, ruta_pdf, ruta_modelo_NER, ruta_regex, ruta_auxiliar):
    with tracer.start_as_current_span("Evaluar artículo") as span:
        root_info = obtener_root_fichero(ruta_info)
        root_regex = obtener_root_fichero(ruta_regex)
        root_auxiliar = obtener_root_fichero(ruta_auxiliar)
        fecha_publicacion = obtener_metadato(root_info, 'fecha_publicacion').text
        stopwords = leer_stopwords()

        ents_reglas = extraccion_reglas.obtener_campos_reglas(dia, ruta_info, ruta_texto, ruta_regex)
        keys_reglas = ents_reglas.keys()
        ents_ner = extraccion_ner.obtener_campos_ner(ruta_texto, ruta_modelo_NER)
        keys_ner = ents_ner.keys()
        puestos_tablas = quitar_puestos_con_inicio_stopword(extraccion_tablas.obtener_puestos(ruta_pdf, ruta_auxiliar), stopwords)

        escribir_id_orden = obtener_metadato(root_info, 'id_orden') is None and 'id_orden' in keys_reglas and ents_reglas['id_orden']
        if escribir_id_orden:
            id_orden = ents_reglas['id_orden'][0].split(' ')[1]

        escribir_fdisp = obtener_metadato(root_info, 'fecha_disposicion') is None
        if escribir_fdisp:
            fecha_disposicion = obtener_fecha_disposicion(
                ents_reglas.get('fecha_disposicion', []),
                fecha_publicacion,
                root_auxiliar
            )

        escala = evitar_extremo_stopword(
            primero_por_preferencia(
                quitar_escalas_incorrectas(limpiar_por_texto(ents_ner.get('escala', []), 'escala')),
                quitar_escalas_incorrectas(ents_reglas.get('escala', []))
            ),
            stopwords
        ).capitalize()

        subescala = evitar_extremo_stopword(
            primero_por_preferencia(
                limpiar_por_texto(ents_ner.get('subescala', []), 'subescala'),
                ents_reglas.get('subescala', [])
            ),
            stopwords
        ).capitalize()

        cuerpo = evitar_extremo_stopword(
            primero_por_preferencia(
                quitar_cuerpos_incorrectos(limpiar_por_texto(ents_ner.get('cuerpo', []), 'cuerpo')),
                quitar_cuerpos_incorrectos(ents_reglas.get('cuerpo', []))
            ),
            stopwords
        ).capitalize()

        grupo = juntar_grupos(
            limpiar_grupos(ents_reglas.get('grupo', [])),
            limpiar_grupos(ents_ner.get('grupo', []))
        )

        tipo_convocatoria = obtener_tipo(
            root_regex,
            ents_reglas.get('tipo_convocatoria_titulo', []),
            ents_reglas.get('tipo_convocatoria_texto', []),
            ents_ner.get('tipo_convocatoria', [])
        )

        datos_contacto = obtener_datos_contacto(
            ents_reglas.get('web', []),
            ents_reglas.get('email', []),
            ents_ner.get('web', []),
            ents_ner.get('email', [])
        )

        plazo = primero_por_preferencia(
            ents_reglas['plazo'] if 'plazo' in keys_reglas else [],
            ents_ner['plazo'] if 'plazo' in keys_ner else []
        )

        fecha_inicio_presentacion, fecha_fin_presentacion = obtener_fechas_presentacion(root_regex, plazo, fecha_publicacion)

        puestos = obtener_puestos(
            ents_reglas.get('puesto', []),
            ents_ner.get('puesto', []),
            puestos_tablas,
            stopwords
        )

        num_plazas = obtener_num_plazas(
            ents_reglas.get('num_plazas_titulo', []),
            ents_reglas.get('num_plazas_texto', []),
            ents_ner.get('num_plazas', []),
            len(puestos)
        )

        articulo = root_info.find('articulo')
        if escribir_id_orden:
            articulo = escribir_en_info(articulo, 'id_orden', id_orden)
        if escribir_fdisp:
            articulo = escribir_en_info(articulo, 'fecha_disposicion', fecha_disposicion)
        articulo = escribir_en_info(articulo, 'escala', escala)
        articulo = escribir_en_info(articulo, 'subescala', subescala)
        articulo = escribir_en_info(articulo, 'cuerpo', cuerpo)
        articulo = escribir_en_info(articulo, 'grupo', grupo)
        articulo = escribir_en_info(articulo, 'tipo_convocatoria', tipo_convocatoria)
        articulo = escribir_en_info(articulo, 'datos_contacto', datos_contacto)
        articulo = escribir_en_info(articulo, 'plazo', plazo)
        articulo = escribir_en_info(articulo, 'fecha_inicio_presentacion', fecha_inicio_presentacion)
        articulo = escribir_en_info(articulo, 'fecha_fin_presentacion', fecha_fin_presentacion)
        articulo = escribir_en_info(articulo, 'num_plazas', str(num_plazas))
        articulo = escribir_puestos_en_info(articulo, puestos)

        with open(ruta_info, 'wb') as file:
            tree_info = ET.ElementTree(root_info)
            tree_info.write(file)
        span.set_attribute("articulo_evaluado", True)

# Extrae los campos de todos los ficheros de directorio_base / dia, guardando
# las extracciones en sus respectivos ficheros de info.
def evaluar_todos(dia, directorio_base, ruta_modelo_NER, ruta_regex, ruta_auxiliar):
    with tracer.start_as_current_span("Evaluar todos los artículos") as span:
        try:
            for filename in os.listdir(directorio_base / dia / 'apertura' / 'txt'):
                if '_legible' not in filename:
                    ruta_texto = directorio_base / dia / 'apertura' / 'txt' / filename
                    ruta_pdf = directorio_base / dia / 'apertura' / 'pdf' / (filename.replace('.txt', '.pdf'))
                    ruta_info = directorio_base / dia / 'apertura' / 'info' / (filename.replace('.txt', '.xml'))
                    
                    try:
                        evaluar_articulo(dia, ruta_info, ruta_texto, ruta_pdf,
                                         ruta_modelo_NER, ruta_regex, ruta_auxiliar)
                        logger.info(f"Artículo evaluado exitosamente: {filename}")
                    except Exception as e:
                        logger.exception(f"Error al evaluar artículo apertura: {filename} - {e}")
                        fichero_log = Path(__file__).absolute().parent.parent / 'articulos_erroneos.log'
                        with open(fichero_log, 'a') as file:
                            file.write(f'{filename.split(".")[0]} - EXTRACCION\n')

            for filename in os.listdir(directorio_base / dia / 'cierre' / 'info'):
                ruta_info = directorio_base / dia / 'cierre' / 'info' / filename
                ruta_pdf = directorio_base / dia / 'cierre' / 'pdf' / (filename.replace('.xml', '.pdf'))
                ruta_texto = directorio_base / dia / 'cierre' / 'txt' / (filename.replace('.xml', '.txt'))
                
                try:
                    evaluar_tablas_cierre(dia, ruta_info, ruta_texto, ruta_pdf,
                                          ruta_regex, ruta_auxiliar, ruta_modelo_NER)
                    logger.info(f"Tablas de cierre evaluadas exitosamente: {filename}")
                except Exception as e:
                    logger.exception(f"Error al evaluar tablas de cierre: {filename} - {e}")
                    fichero_log = Path(__file__).absolute().parent.parent / 'articulos_erroneos.log'
                    with open(fichero_log, 'a') as file:
                        file.write(f'{filename.split(".")[0]} - EXTRACCION\n')

        except Exception as e:
            logger.exception(f"Error en la evaluación de todos los artículos: {e}")
            span.record_exception(e)
            span.set_status(trace.StatusCode.ERROR)

        span.set_attribute("dia", dia)
        span.set_attribute("directorio_base", str(directorio_base))

# Evalúa únicamente el artículo del caso indicado, contando previamente
# con una estructura definida.
def evaluar_pruebas_aceptacion(caso):
    with tracer.start_as_current_span("Evaluar pruebas de aceptación") as span:
        try:
            ruta_modelo_NER = Path(r'C:\AragonOpenData\aragon-opendata\models\modelo_ner')
            ruta_regex = Path(r'C:\AragonOpenData\aragon-opendata\tools\ficheros_configuracion\regex.xml')
            ruta_auxiliar = Path(r'C:\AragonOpenData\aragon-opendata\tools\ficheros_configuracion\auxiliar.xml')
            ruta_casos = Path(r'C:\Users\opotrony\Desktop\Artículos de casos de prueba')

            cwd = ruta_casos / ('Caso_' + caso)
            ruta_pdf, ruta_info, ruta_txt = None, None, None
            for file in os.listdir(cwd):
                if '.pdf' in file:
                    ruta_pdf = cwd / file
                elif '.xml' in file and 'copia' not in file:
                    ruta_info = cwd / file
                elif '.txt' in file:
                    ruta_txt = cwd / file

            if 'cierre' in caso.lower():
                dia = ruta_pdf.name.split('_')[1]
                try:
                    evaluar_tablas_cierre(dia, ruta_info, ruta_txt, ruta_pdf, ruta_regex, ruta_auxiliar, ruta_modelo_NER)
                    logger.info(f"Tablas de cierre evaluadas para caso: {caso}")
                except Exception as e:
                    logger.exception(f"Error al evaluar tablas de cierre para caso {caso}: {e}")
            else:
                try:
                    dia = ruta_pdf.name.split('_')[-2]
                    evaluar_articulo(dia, ruta_info, ruta_txt, ruta_pdf, ruta_modelo_NER, ruta_regex, ruta_auxiliar)
                    logger.info(f"Artículo evaluado para caso: {caso}")
                except Exception as e:
                    logger.exception(f"Error al evaluar artículo para caso {caso}: {e}")

        except Exception as e:
            logger.exception(f"Error en la evaluación de pruebas de aceptación: {e}")
            span.record_exception(e)
            span.set_status(trace.StatusCode.ERROR)

        span.set_attribute("caso", caso)

def main():
    with tracer.start_as_current_span("Extraccion main") as span:
            span.set_attribute("dag_id", dag_id)

            # Validar el número de parámetros
            if len(sys.argv) == 3:
                logger.info(f"Evaluating acceptance tests for {sys.argv[1]}")
                with tracer.start_as_current_span("Evaluate Acceptance Tests") as eval_span:
                    try:
                        evaluar_pruebas_aceptacion(sys.argv[1])
                        logger.info("Acceptance tests evaluated successfully.")
                    except Exception as e:
                        logger.error(f"Error during acceptance test evaluation: {e}")
                        eval_span.record_exception(e)
                        eval_span.set_status(trace.status.StatusCode.ERROR)

            elif len(sys.argv) != 7:
                error_msg = 'Numero de parametros incorrecto.'
                logger.error(error_msg)
                span.set_status(trace.status.StatusCode.ERROR)
                return
            else:
                # Capturar parámetros para la evaluación completa
                dia = sys.argv[1]
                directorio_base = Path(sys.argv[2])
                ruta_modelo_NER = Path(sys.argv[3])
                ruta_regex = Path(sys.argv[4])
                ruta_auxiliar = Path(sys.argv[5])

                logger.info(f"Starting full evaluation for {dia} with provided paths.")
                span.set_attribute("dia", dia)
                span.set_attribute("directorio_base", str(directorio_base))
                span.set_attribute("ruta_modelo_NER", str(ruta_modelo_NER))
                span.set_attribute("ruta_regex", str(ruta_regex))
                span.set_attribute("ruta_auxiliar", str(ruta_auxiliar))

                # Evaluar todos los artículos
                with tracer.start_as_current_span("Evaluate All") as eval_all_span:
                    try:
                        evaluar_todos(dia, directorio_base, ruta_modelo_NER, ruta_regex, ruta_auxiliar)
                        logger.info("All evaluations completed successfully.")
                    except Exception as e:
                        logger.error(f"Error during full evaluation: {e}")
                        eval_all_span.record_exception(e)
                        eval_all_span.set_status(trace.status.StatusCode.ERROR)

if __name__ == "__main__":
	main()
