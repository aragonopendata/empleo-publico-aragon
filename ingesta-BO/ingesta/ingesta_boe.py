import sys
import requests
import logging
from pathlib import Path
from xml.etree import ElementTree as ET
from datetime import datetime
import calendar
import locale
from PyPDF2 import PdfFileReader, PdfFileWriter
import os

from opentelemetry import trace

# Configurar la localización
locale.setlocale(locale.LC_ALL, 'es_ES.UTF-8')

class IngestaBOE:
    def __init__(self, tracer, logger):
        self.tracer = tracer
        self.logger = logger

    def recuperar_strings(self, tipo):
        with self.tracer.start_as_current_span(f"Recuperar strings {tipo}") as span:
            ruta_fcs = Path(__file__).absolute().parent.parent / 'ficheros_configuracion'
            ruta_fichero_aux = ruta_fcs / 'auxiliar.xml'
            span.set_attribute("file.path", str(ruta_fichero_aux))

            try:
                with open(ruta_fichero_aux, 'rb') as file:
                    tree_fa = ET.parse(file)
                    root_fa = tree_fa.getroot()
            except Exception as e:
                msg = f"Failed to open {ruta_fichero_aux}"
                self.logger.exception(msg)
                span.set_status(trace.status.StatusCode.ERROR)
                return []

            item = root_fa.find('./strings_' + tipo)
            out = []
            for i in item.findall('./string'):
                out.append(i.text)
            span.set_attribute("strings.recovered", out)
            return out

    def rotar_pdf(self, path_in, path_out):
        with self.tracer.start_as_current_span("Rotar PDF") as span:
            span.set_attribute("file.path_in", str(path_in))
            span.set_attribute("file.path_out", str(path_out))

            try:
                pdf_in = open(path_in, 'rb')
                pdf_reader = PdfFileReader(pdf_in)
                span.set_attribute("pdf.pages_count", len(pdf_reader.pages))
            except Exception as e:
                msg = f"Failed to read {path_in}"
                self.logger.exception(msg)
                span.record_exception(e)
                span.set_status(trace.status.StatusCode.ERROR)
                return

            # Rotar 90º página por página e incorporar al PDF de salida
            pdf_writer = PdfFileWriter()
            for pagenum in range(pdf_reader.numPages):
                page = pdf_reader.getPage(pagenum)
                page.rotateClockwise(90)
                pdf_writer.addPage(page)

            try:
                pdf_out = open(path_out, 'wb')
                pdf_writer.write(pdf_out)
                pdf_out.close()
                span.set_status(trace.status.StatusCode.OK)
                self.logger.info(f"PDF rotado y guardado en {path_out}")
            except Exception as e:
                msg = f"Failed to write {path_out}"
                self.logger.exception(msg)
                span.record_exception(e)
                span.set_status(trace.status.StatusCode.ERROR)
                return

    def ingesta_diaria_boe(self, dia, directorio_base):
        with self.tracer.start_as_current_span("Ingesta diaria BOE") as span:
            ruta_fcs = Path(__file__).absolute().parent.parent / 'ficheros_configuracion'
            ruta_fichero_conf = ruta_fcs / 'BOE_conf.xml'
            span.set_attribute("file.path", str(ruta_fichero_conf))

            try:
                with open(ruta_fichero_conf, 'rb') as file:
                    tree_fc = ET.parse(file)
                    root_fc = tree_fc.getroot()
                    self.logger.info(f"Archivo de configuración leído exitosamente desde {ruta_fichero_conf}")
                    span.set_attribute("config_file.loaded", True)
            except Exception as e:
                msg = f"Failed to open {ruta_fichero_conf}"
                self.logger.exception(msg)
                span.record_exception(e)
                span.set_status(trace.status.StatusCode.ERROR)
                return

            diaSemana = list(calendar.day_name)[datetime.strptime(dia, '%Y%m%d').weekday()]
            span.set_attribute("dia_semana", diaSemana)
            prefijo_url_sumario = root_fc.find('./prefijo_url_sumario').text
            url = prefijo_url_sumario + dia
            prefijo_url = root_fc.find('./prefijo_url').text

            # Obtener XML sumario
            with self.tracer.start_as_current_span("Obtener XML Sumario") as span_sumario:
                span_sumario.set_attribute("request.url", url)
                try:
                    headers = {"Accept": "application/xml"}
                    response = requests.get(url, headers=headers)
                    
                    contenido = response.content
                    span_sumario.set_attribute("response.status_code", response.status_code)
                except Exception as e:
                    msg = f"Failed to request {url}"
                    self.logger.exception(msg)
                    span_sumario.record_exception(e)
                    span_sumario.set_status(trace.status.StatusCode.ERROR)
                    return

            # Chequear que en el día indicado ha habido BOE
            with self.tracer.start_as_current_span("Chequear BOE en día indicado") as span_chequeo:
                try:
                    # Cambio: La estructura del XML ha cambiado, ahora tiene una raíz 'response'
                    root_check = ET.fromstring(contenido)
                    # Comprobamos que el código de estado es 200 (ok)
                    status_code = root_check.find('./status/code')
                    if status_code is None or status_code.text != '200':
                        raise AssertionError(f"El código de estado no es 200, el dia {dia} es {diaSemana}")
                    
                    # Verificar que hay datos en el sumario
                    data = root_check.find('./data/sumario')
                    if data is None:
                        raise AssertionError(f"No hay datos en el sumario, el dia {dia} es {diaSemana}")
                except Exception as e:
                    msg = f"Failed assert: {str(e)}"
                    self.logger.exception(msg)
                    span_chequeo.record_exception(e)
                    span_chequeo.set_status(trace.status.StatusCode.ERROR)
                    return

            # Guardar XML sumario
            with self.tracer.start_as_current_span("Guardar XML Sumario") as span_guardar_sumario:
                try:
                    nombre_sumario = 'BOE_Sumarizado_' + dia + '.xml'
                    ruta_sumario = directorio_base / dia / nombre_sumario
                    span_guardar_sumario.set_attribute("file.path", str(ruta_sumario))

                    with open(ruta_sumario, 'wb') as file:
                        file.write(contenido)
                    span_guardar_sumario.set_status(trace.status.StatusCode.OK)
                    self.logger.info(f"XML sumario guardado en {ruta_sumario}")
                except Exception as e:
                    msg = f"Failed to write XML content to {ruta_sumario}"
                    self.logger.exception(msg)
                    span_guardar_sumario.record_exception(e)
                    span_guardar_sumario.set_status(trace.status.StatusCode.ERROR)
                    return

            # Leer y parsear fichero sumario
            with self.tracer.start_as_current_span("Leer y parsear fichero sumario") as span_parseo:
                try:
                    with open(ruta_sumario, 'rb') as file:
                        tree = ET.parse(file)
                        root = tree.getroot()
                except Exception as e:
                    msg = f"Failed to open {ruta_sumario}"
                    self.logger.exception(msg)
                    span_parseo.record_exception(e)
                    span_parseo.set_status(trace.status.StatusCode.ERROR)
                    return

            strings_apertura = self.recuperar_strings('apertura')
            strings_cierre = self.recuperar_strings('cierre')
            strings_no_empleo = self.recuperar_strings('no_empleo')
            indice = 1

            # Actualizar las rutas XPath para adaptarse a la nueva estructura XML
            oposiciones_path = './data/sumario/diario/seccion[@codigo="2B"]/departamento/epigrafe/item'
            nombramientos_path = './data/sumario/diario/seccion[@codigo="2A"]/departamento/epigrafe/item'
            
            # Por cada artículo de las secciones que nos interesan del fichero de configuración:
            for item in root.findall(oposiciones_path) + root.findall(nombramientos_path):
                
                with self.tracer.start_as_current_span("Procesar artículo del sumario") as span_procesar:
                    # Actualizar los XPath para acceder a los elementos en la nueva estructura
                    tit = item.find('./titulo').text
                    no_es_empleo = False
                    for cadena in strings_no_empleo:
                        if cadena in tit.lower():    # Si no es de empleo, no guardar artículo
                            no_es_empleo = True
                    if no_es_empleo:
                        continue
                    es_apertura = False
                    es_cierre = False
                    for cadena in strings_apertura:
                        if cadena in tit.lower():
                            es_apertura = True
                            break
                    if es_apertura:            # Guardar en apertura
                        tipo_articulo = 'apertura'
                    else:
                        for cadena in strings_cierre:
                            if cadena in tit.lower():
                                es_cierre = True
                                break
                        if es_cierre:        # Guardar en cierre
                            tipo_articulo = 'cierre'
                        else:
                            continue        # Saltar el artículo

                    nombre_fichero = 'BOE_' + dia + '_' + str(indice)

                    # Obtener y guardar ficheros de los distintos formatos indicados en el fichero de configuración.
                    formatos = []
                    for t in root_fc.find('url_formatos').iter():
                        formatos.append(t.tag)
                    formatos = formatos[1:]        # Quitar el propio tag de url_formatos
                    if 'xml' in formatos:        # Poner el xml primero
                        formatos.remove('xml')
                        formatos.insert(0,'xml')

                    siguiente_iteracion = False
                    for formato in formatos:
                        nombre_formato_fichero = nombre_fichero + '.' + formato
                        ruta_fichero = directorio_base / dia / tipo_articulo / formato / nombre_formato_fichero
                        
                        # Actualizar las rutas a los distintos formatos
                        if formato == 'pdf':
                            url_attr = 'url_pdf'
                        elif formato == 'xml':
                            url_attr = 'url_xml'
                        elif formato == 'html':
                            url_attr = 'url_html'
                        else:
                            url_attr = f'url_{formato}'
                        
                        url = item.find(f'./{url_attr}').text

                        with self.tracer.start_as_current_span(f"Guardar archivo {formato}") as span_guardar_formato:
                            span_guardar_formato.set_attribute("file.path", str(ruta_fichero))
                            span_guardar_formato.set_attribute("request.url", url)

                            try:
                                contenido_url = requests.get(url).content
                                if formato == 'xml':
                                    root_tmp = ET.fromstring(contenido_url)
                                    # Actualizar la ruta al elemento rango
                                    rango_encontrado = root_tmp.find('./metadatos/rango').text
                                    if rango_encontrado.lower() not in ['resolución', 'resolucion', 'orden']:
                                        siguiente_iteracion = True
                                        break
                                with open(ruta_fichero, 'wb') as file:
                                    file.write(contenido_url)
                                self.logger.info(f"Archivo {formato} guardado en {ruta_fichero}")
                            except Exception as e:
                                msg = f"Failed to write {ruta_fichero} from {url}"
                                self.logger.exception(msg)
                                span_guardar_formato.record_exception(e)
                                span_guardar_formato.set_status(trace.status.StatusCode.ERROR)
                                siguiente_iteracion = True
                                break
                                    
                        if formato == 'html':
                            url_html = url
                                
                    if siguiente_iteracion:
                        continue

                    # Creación del fichero de info e inserción de los datos de que se dispone
                    root_info = ET.Element('root')
                    articulo = ET.Element('articulo')
                    root_info.append(articulo)
                    fuente_datos = ET.SubElement(articulo, 'fuente_datos')
                    fuente_datos.text = 'BOE'
                    fecha_publicacion = ET.SubElement(articulo, 'fecha_publicacion')
                    fecha_publicacion.text = dia[-2:] + '/' + dia[4:6] + '/' + dia[:4]
                    enlace = ET.SubElement(articulo, 'enlace_convocatoria')
                    enlace.text = url_html

                    # Lectura del XML del artículo para obtener etiquetas
                    nombre_xml_fichero = nombre_fichero + '.xml'
                    with open(directorio_base / dia / tipo_articulo / 'xml' / nombre_xml_fichero,'rb') as file:
                        tree_aux = ET.parse(file)
                        root_aux = tree_aux.getroot()

                    # Lectura de etiquetas a guardar
                    etiquetas = []
                    for i in root_fc.find('./etiquetas_xml/a_guardar').iter():
                        etiquetas.append((i.tag, i.text))
                    etiquetas = etiquetas[1:]

                    buscar_id_orden = 'rango' in [e[0] for e in etiquetas] and \
                                            root_aux.find('./metadatos/rango').text.lower() == 'orden'

                    # Obtención de textos de las etiquetas (actualizando rutas)
                    for et_tag, et_text in etiquetas:
                        # Actualizar la ruta para que apunte a los metadatos
                        et_text_updated = et_text.replace('./metadatos/', './metadatos/')
                        
                        if et_tag == 'fecha_disposicion':
                            SE = ET.SubElement(articulo, et_tag)
                            el = root_aux.find(et_text_updated)
                            SE.text = '-' if el is None else el.text[-2:]+'/'+el.text[4:6]+'/'+el.text[:4]
                        elif et_tag == 'id_orden':
                            if buscar_id_orden:
                                SE = ET.SubElement(articulo, et_tag)
                                el = root_aux.find(et_text_updated)
                                SE.text = el.text if el is not None else '-'
                        else:
                            SE = ET.SubElement(articulo, et_tag)
                            el = root_aux.find(et_text_updated)
                            if et_tag == 'organo_convocante':
                                SE.text = el.text.upper() if el is not None else '-'
                            else:
                                SE.text = el.text if el is not None else '-'                

                    if 'uri_eli' not in [e[0] for e in etiquetas]:
                        SE = ET.SubElement(articulo, 'uri_eli')
                        SE.text = '-'

                    tree_info = ET.ElementTree(root_info)

                    with open(directorio_base / dia / tipo_articulo / 'info' / nombre_xml_fichero,'wb') as file:
                        tree_info.write(file)

                    # Crear pdf rotado
                    nombre_pdf = nombre_fichero + '.pdf'
                    ruta_pdf = directorio_base / dia / tipo_articulo / 'pdf' / nombre_pdf
                    ruta_pdf_rotado = directorio_base / dia / tipo_articulo / 'pdf' / 'rotados' / nombre_pdf
                    self.rotar_pdf(ruta_pdf, ruta_pdf_rotado)

                    indice += 1