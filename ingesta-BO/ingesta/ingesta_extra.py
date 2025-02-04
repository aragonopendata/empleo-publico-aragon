import sys
import requests
import logging
from pathlib import Path
from xml.etree import ElementTree as ET
from datetime import datetime
import calendar
import locale

from opentelemetry import trace

# Configurar la localización
locale.setlocale(locale.LC_ALL, 'es_ES.UTF-8')

class IngestaExtra:
    def __init__(self, tracer, logger):
        self.tracer = tracer
        self.logger = logger

    def recuperar_strings(self, tipo):
        with self.tracer.start_as_current_span("Recuperar strings") as span:
            ruta_fcs = Path(__file__).absolute().parent.parent / 'ficheros_configuracion'
            ruta_fichero_aux = ruta_fcs / 'auxiliar.xml'
            span.set_attribute("file.path", str(ruta_fichero_aux))

            try:
                with open(ruta_fichero_aux, 'rb') as file:
                    tree_fa = ET.parse(file)
                    root_fa = tree_fa.getroot()
                    self.logger.info(f"Archivo auxiliar leído exitosamente desde {ruta_fichero_aux}")
            except Exception as e:
                msg = f"Failed to open {ruta_fichero_aux}"
                self.logger.exception(msg)
                span.record_exception(e)
                span.set_status(trace.status.StatusCode.ERROR)
                return []

            item = root_fa.find('./strings_' + tipo)
            out = []
            for i in item.findall('./string'):
                out.append(i.text)
            span.set_attribute("strings.recovered", out)
            return out

    def ingesta_diaria_extra(self, dia, directorio_base, tipo_boletin):
        with self.tracer.start_as_current_span(f"Ingesta diaria extra: {tipo_boletin}") as span:
            # Recuperar el fichero de configuración
            ruta_fcs = Path(__file__).absolute().parent.parent / 'ficheros_configuracion'
            ruta_fichero_conf = ruta_fcs / (tipo_boletin + '_conf.xml')
            span.set_attribute("file.path", str(ruta_fichero_conf))

            try:
                with open(ruta_fichero_conf, 'rb') as file:
                    tree_fc = ET.parse(file)
                    root_fc = tree_fc.getroot()
                    self.logger.info(f"Archivo de configuración leído exitosamente desde {ruta_fichero_conf}")
            except Exception as e:
                msg = f"Failed to open {ruta_fichero_conf}"
                self.logger.exception(msg)
                span.record_exception(e)
                span.set_status(trace.status.StatusCode.ERROR)
                return

            diaSemana = list(calendar.day_name)[datetime.strptime(dia, '%Y%m%d').weekday()]
            span.set_attribute("dia_semana", diaSemana)
            
            # Ha de recibir un XML Sumario
            prefijo_url_sumario = root_fc.find('./prefijo_url_sumario').text
            url = prefijo_url_sumario + dia  # Se ha decidido poner como en el BOE
            span.set_attribute("request.url", url)

            try:
                response = requests.get(url)
                contenido = response.content
            except Exception as e:
                msg = f"Failed to request {url}"
                self.logger.exception(msg)
                span.record_exception(e)
                span.set_status(trace.status.StatusCode.ERROR)
                return

            # Chequear que en el día indicado ha habido Boletín (si el fichero de configuración lo permite)
            root_check = ET.fromstring(contenido)
            raiz = root_fc.find('./etiquetas_xml_sumario/raiz')
            if raiz is not None:
                try:
                    assert root_check.tag == raiz.text
                except Exception as e:
                    msg = f"Failed assert: El tag del root es {root_check.tag}, el dia {dia} es {diaSemana}"
                    self.logger.exception(msg)
                    span.record_exception(e)
                    span.set_status(trace.status.StatusCode.ERROR)
                    return

            # Guardar XML sumario
            try:
                nombre_sumario = tipo_boletin + '_Sumario_' + dia + '.xml'
                ruta_sumario = directorio_base / dia / nombre_sumario
                with open(ruta_sumario, 'wb') as file:
                    file.write(contenido)
                self.logger.info(f"XML sumarizado guardado en {ruta_sumario}")
                span.set_attribute("file.path", str(ruta_sumario))
            except Exception as e:
                msg = f"Failed to write content from {url} to {ruta_sumario}"
                self.logger.exception(msg)
                span.record_exception(e)
                span.set_status(trace.status.StatusCode.ERROR)
                return

            # Leer y parsear fichero sumario
            try:
                with open(ruta_sumario, 'rb') as file:
                    tree = ET.parse(file)
                    root = tree.getroot()
                span.set_attribute("xml_parsed", True)
            except Exception as e:
                msg = f"Failed to open {ruta_sumario}"
                self.logger.exception(msg)
                span.record_exception(e)
                span.set_status(trace.status.StatusCode.ERROR)
                return

            elementos_secciones = []
            secc = root_fc.find('./secciones_xml')
            if secc is not None:
                # Recuperar de qué secciones se quiere obtener la información del sumario
                for e in secc.iter():
                    elementos_secciones.append(e)
                elementos_secciones = elementos_secciones[1:]
            else:
                # Recuperar todos los elementos de la etiqueta indicada como registro en el fichero de configuración
                for e in root.findall(root_fc.find('./etiquetas_xml_sumario/registro')):
                    elementos_secciones.append(e)

            # Preparar etiquetas que indica el fichero de configuración que se pueden obtener de cada ítem.
            etiquetas = []
            for e in root_fc.find('./etiquetas_xml/a_guardar').iter():
                etiquetas.append(e)
            etiquetas = etiquetas[1:]

            strings_apertura = recuperar_strings('apertura')
            strings_cierre = recuperar_strings('cierre')
            indice = 1

            # Leer los distintos registros
            for item in elementos_secciones:

                # Mediante título o texto (si no hay título), dividir en apertura y cierre.
                # Si no están estos, no se guarda (innecesario un artículo sin título ni texto).
                if 'titulo' in etiquetas:
                    t = item.find(root_fc.find('./etiquetas_xml/a_guardar/titulo').text).text
                elif 'texto' in etiquetas:
                    t = item.find(root_fc.find('./etiquetas_xml/a_guardar/texto').text).text
                else:
                    continue

                es_apertura, es_cierre = False, False
                for cadena in strings_apertura:
                    if cadena in t.lower():
                        es_apertura = True
                        break
                if es_apertura:
                    tipo_articulo = 'apertura'
                else:
                    for cadena in strings_cierre:
                        if cadena in t.lower():
                            es_cierre = True
                            break
                    if es_cierre:
                        tipo_articulo = 'cierre'
                    else:
                        continue

                # Almacenamiento de los ficheros en distintos formatos
                nombre_fichero = tipo_boletin + '_' + dia + '_' + str(indice)
                hay_html, hay_pdf, hay_xml = False, False, False
                for etiqueta in etiquetas:
                    if 'htm' in etiqueta.lower() or 'url' == etiqueta.lower():
                        hay_html = True
                        et_html = etiqueta
                        nombre_fichero_html = nombre_fichero + '.html'
                        # No se elimina porque interesa para el campo "enlace a la convocatoria"
                    elif 'pdf' in etiqueta.lower():
                        hay_pdf = True
                        et_pdf = etiqueta
                        nombre_fichero_pdf = nombre_fichero + '.pdf'
                        # etiquetas.remove(etiqueta)
                    elif 'xml' in etiqueta.lower():
                        hay_xml = True
                        et_xml = etiqueta
                        nombre_fichero_xml = nombre_fichero + '.xml'
                        # etiquetas.remove(etiqueta)

                # Guardar XML (de URL si lo hay y el propio ítem si no lo hay)
                ruta_fichero_xml = directorio_base / dia / tipo_articulo / 'xml' / nombre_fichero_xml
                if hay_xml:
                    url = item.find(root_fc.find('./etiquetas_xml/a_guardar/' + et_xml).text).text
                    contenido_url = requests.get(url).content
                    with open(ruta_fichero_xml, 'wb') as file:
                            file.write(contenido_url)
                else:												# Guardar parte del XML sumario con el registro como root
                    nombre_fichero_xml = nombre_fichero + '.xml'
                    tree_articulo = ET.ElementTree(item)
                    with open(ruta_fichero_xml,'wb') as file:
                        tree_articulo.write(file)

                # Guardar PDF si lo hay
                if hay_pdf:
                    url = item.find(root_fc.find('./etiquetas_xml/a_guardar/' + et_pdf).text).text
                    contenido_url = requests.get(url).content
                    with open(directorio_base / dia / tipo_articulo / 'pdf' / nombre_fichero_pdf, 'wb') as file:
                            file.write(contenido_url)

                # Guardar HTML si lo hay
                if hay_html:
                    url = item.find(root_fc.find('./etiquetas_xml/a_guardar/' + et_html).text).text
                    contenido_url = requests.get(url).content
                    with open(directorio_base / dia / tipo_articulo / 'html' / nombre_fichero_html, 'wb') as file:
                            file.write(contenido_url)
                
                # Creación del fichero de info e inserción de los datos de que se dispone
                root_info = ET.Element('root')
                articulo = ET.Element('articulo')
                root_info.append(articulo)
                
                # Lectura del XML almacenado
                with open(ruta_fichero_xml,'rb') as file:
                    tree_aux = ET.parse(file)
                    root_aux = tree_aux.getroot()

                # Guardar en lista, con mismo índice que la etiqueta, los textos de las mismas encontrados en el XML.
                texto_etiquetas = []
                for etiqueta in etiquetas:
                    texto_etiquetas.append(root_aux.find(root_fc.find('./etiquetas_xml/a_guardar/'+etiqueta).text).text)

                # Incorporar etiquetas y textos al árbol
                for i, etiqueta in enumerate(etiquetas):
                    elemento_auxiliar = ET.SubElement(articulo, etiqueta)
                    elemento_auxiliar.text = texto_etiquetas[i]

                # Guardar el fichero de info
                tree_info = ET.ElementTree(root_info)
                with open(directorio_base / dia / tipo_articulo / 'info' / nombre_fichero_xml,'wb') as file:
                    tree_info.write(file)

                indice += 1

    def preparar_elementos_secciones(self, root, root_fc):
        with self.tracer.start_as_current_span("Preparar elementos de secciones") as span:
            elementos_secciones = []
            secc = root_fc.find('./secciones_xml')
            if secc is not None:
                for e in secc.iter():
                    elementos_secciones.append(e)
                elementos_secciones = elementos_secciones[1:]
            else:
                for e in root.findall(root_fc.find('./etiquetas_xml_sumario/registro')):
                    elementos_secciones.append(e)
            span.set_attribute("elementos_secciones", len(elementos_secciones))
            return elementos_secciones

    def preparar_etiquetas(self, root_fc):
        with self.tracer.start_as_current_span("Preparar etiquetas") as span:
            etiquetas = []
            for e in root_fc.find('./etiquetas_xml/a_guardar').iter():
                etiquetas.append(e.tag)
            etiquetas = etiquetas[1:]
            span.set_attribute("etiquetas_preparadas", etiquetas)
            return etiquetas

    def procesar_registros(self, dia, directorio_base, tipo_boletin, elementos_secciones, etiquetas, root_fc):
        with self.tracer.start_as_current_span("Procesar registros del sumario") as span:
            strings_apertura = self.recuperar_strings('apertura')
            strings_cierre = self.recuperar_strings('cierre')
            indice = 1

            for item in elementos_secciones:
                es_apertura, es_cierre = False, False
                t = None
                if 'titulo' in etiquetas:
                    t = item.find(root_fc.find('./etiquetas_xml/a_guardar/titulo').text).text
                elif 'texto' in etiquetas:
                    t = item.find(root_fc.find('./etiquetas_xml/a_guardar/texto').text).text
                if not t:
                    continue

                for cadena in strings_apertura:
                    if cadena in t.lower():
                        es_apertura = True
                        break
                if es_apertura:
                    tipo_articulo = 'apertura'
                else:
                    for cadena in strings_cierre:
                        if cadena in t.lower():
                            es_cierre = True
                            break
                    if es_cierre:
                        tipo_articulo = 'cierre'
                    else:
                        continue

                self.almacenar_ficheros(dia, directorio_base, tipo_boletin, tipo_articulo, item, etiquetas, root_fc, indice)
                indice += 1

    def almacenar_ficheros(self, dia, directorio_base, tipo_boletin, tipo_articulo, item, etiquetas, root_fc, indice):
        with self.tracer.start_as_current_span("Almacenar ficheros") as span:
            nombre_fichero = tipo_boletin + '_' + dia + '_' + str(indice)
            hay_html, hay_pdf, hay_xml = False, False, False

            for etiqueta in etiquetas:
                if 'htm' in etiqueta.lower() or 'url' == etiqueta.lower():
                    hay_html = True
                    et_html = etiqueta
                    nombre_fichero_html = nombre_fichero + '.html'
                elif 'pdf' in etiqueta.lower():
                    hay_pdf = True
                    et_pdf = etiqueta
                    nombre_fichero_pdf = nombre_fichero + '.pdf'
                elif 'xml' in etiqueta.lower():
                    hay_xml = True
                    et_xml = etiqueta
                    nombre_fichero_xml = nombre_fichero + '.xml'

            if hay_xml:
                self.guardar_xml(dia, directorio_base, tipo_articulo, nombre_fichero_xml, item, root_fc, et_xml)
            else:
                self.guardar_xml_item(dia, directorio_base, tipo_articulo, nombre_fichero_xml, item)

            if hay_pdf:
                self.guardar_pdf(dia, directorio_base, tipo_articulo, nombre_fichero_pdf, item, root_fc, et_pdf)

            if hay_html:
                self.guardar_html(dia, directorio_base, tipo_articulo, nombre_fichero_html, item, root_fc, et_html)

            self.crear_fichero_info(dia, directorio_base, tipo_articulo, nombre_fichero, etiquetas, root_fc, item)

    def guardar_xml(self, dia, directorio_base, tipo_articulo, nombre_fichero_xml, item, root_fc, et_xml):
        with self.tracer.start_as_current_span("Guardar XML") as span:
            ruta_fichero_xml = directorio_base / dia / tipo_articulo / 'xml' / nombre_fichero_xml
            url = item.find(root_fc.find('./etiquetas_xml/a_guardar/' + et_xml).text).text
            span.set_attribute("file.path", str(ruta_fichero_xml))
            span.set_attribute("request.url", url)

            try:
                contenido_url = requests.get(url).content
                with open(ruta_fichero_xml, 'wb') as file:
                    file.write(contenido_url)
                self.logger.info(f"Archivo XML guardado en {ruta_fichero_xml}")
            except Exception as e:
                msg = f"Failed to write XML content from {url} to {ruta_fichero_xml}"
                self.logger.exception(msg)
                span.record_exception(e)
                span.set_status(trace.status.StatusCode.ERROR)

    def guardar_xml_item(self, dia, directorio_base, tipo_articulo, nombre_fichero_xml, item):
        with self.tracer.start_as_current_span("Guardar XML Item") as span:
            ruta_fichero_xml = directorio_base / dia / tipo_articulo / 'xml' / nombre_fichero_xml
            span.set_attribute("file.path", str(ruta_fichero_xml))

            try:
                tree_articulo = ET.ElementTree(item)
                with open(ruta_fichero_xml, 'wb') as file:
                    tree_articulo.write(file)
                self.logger.info(f"Item XML guardado en {ruta_fichero_xml}")
            except Exception as e:
                msg = f"Failed to write XML item to {ruta_fichero_xml}"
                self.logger.exception(msg)
                span.record_exception(e)
                span.set_status(trace.status.StatusCode.ERROR)

    def guardar_pdf(self, dia, directorio_base, tipo_articulo, nombre_fichero_pdf, item, root_fc, et_pdf):
        with self.tracer.start_as_current_span("Guardar PDF") as span:
            ruta_fichero_pdf = directorio_base / dia / tipo_articulo / 'pdf' / nombre_fichero_pdf
            url = item.find(root_fc.find('./etiquetas_xml/a_guardar/' + et_pdf).text).text
            span.set_attribute("file.path", str(ruta_fichero_pdf))
            span.set_attribute("request.url", url)

            try:
                contenido_url = requests.get(url).content
                with open(ruta_fichero_pdf, 'wb') as file:
                    file.write(contenido_url)
                self.logger.info(f"Archivo PDF guardado en {ruta_fichero_pdf}")
            except Exception as e:
                msg = f"Failed to write PDF content from {url} to {ruta_fichero_pdf}"
                self.logger.exception(msg)
                span.record_exception(e)
                span.set_status(trace.status.StatusCode.ERROR)

    def guardar_html(self, dia, directorio_base, tipo_articulo, nombre_fichero_html, item, root_fc, et_html):
        with self.tracer.start_as_current_span("Guardar HTML") as span:
            ruta_fichero_html = directorio_base / dia / tipo_articulo / 'html' / nombre_fichero_html
            url = item.find(root_fc.find('./etiquetas_xml/a_guardar/' + et_html).text).text
            span.set_attribute("file.path", str(ruta_fichero_html))
            span.set_attribute("request.url", url)

            try:
                contenido_url = requests.get(url).content
                with open(ruta_fichero_html, 'wb') as file:
                    file.write(contenido_url)
                self.logger.info(f"Archivo HTML guardado en {ruta_fichero_html}")
            except Exception as e:
                msg = f"Failed to write HTML content from {url} to {ruta_fichero_html}"
                self.logger.exception(msg)
                span.record_exception(e)
                span.set_status(trace.status.StatusCode.ERROR)

    def crear_fichero_info(self, dia, directorio_base, tipo_articulo, nombre_fichero, etiquetas, root_fc, item):
        with self.tracer.start_as_current_span("Crear fichero de info") as span:
            ruta_fichero_info = directorio_base / dia / tipo_articulo / 'info' / (nombre_fichero + '.xml')
            span.set_attribute("file.path", str(ruta_fichero_info))

            try:
                root_info = ET.Element('root')
                articulo = ET.Element('articulo')
                root_info.append(articulo)

                for etiqueta in etiquetas:
                    elemento_auxiliar = ET.SubElement(articulo, etiqueta)
                    el = item.find(root_fc.find('./etiquetas_xml/a_guardar/' + etiqueta).text)
                    elemento_auxiliar.text = el.text if el is not None else '-'

                tree_info = ET.ElementTree(root_info)
                with open(ruta_fichero_info, 'wb') as file:
                    tree_info.write(file)
                self.logger.info(f"Archivo de info guardado en {ruta_fichero_info}")
            except Exception as e:
                msg = f"Failed to write info XML to {ruta_fichero_info}"
                self.logger.exception(msg)
                span.record_exception(e)
                span.set_status(trace.status.StatusCode.ERROR)
