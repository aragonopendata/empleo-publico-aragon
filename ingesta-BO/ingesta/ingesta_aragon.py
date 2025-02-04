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

class IngestaAragon:
    def __init__(self, tracer, logger):
        self.tracer = tracer
        self.logger = logger

    def encontrar_cadenas(self, texto, strings):
        with self.tracer.start_as_current_span("Encontrar cadenas") as span:
            contador = 0
            for string in strings:
                if string in texto:
                    contador += 1
            span.set_attribute("cadenas_encontradas", contador)
            return contador

    def recuperar_fichero_configuracion(self, tipo_boletin):
        with self.tracer.start_as_current_span("Recuperar fichero de configuración") as span:
            ruta_fcs = Path(__file__).absolute().parent.parent / 'ficheros_configuracion'
            ruta_fichero_conf = ruta_fcs / (tipo_boletin + '_conf.xml')
            span.set_attribute("file.path", str(ruta_fichero_conf))

            try:
                with open(ruta_fichero_conf, 'rb') as file:
                    tree = ET.parse(file)
                    root = tree.getroot()
                    self.logger.info(f"Archivo de configuración leído exitosamente desde {ruta_fichero_conf}")
                    span.set_attribute("config_file.loaded", True)
                    return root
            except Exception as e:
                msg = f"Failed to open {ruta_fichero_conf}"
                self.logger.exception(msg)
                span.record_exception(e)
                span.set_status(trace.status.StatusCode.ERROR)
                return None

    def recuperar_strings(self, tipo, bops=False):
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

            str_aux = './strings_' + tipo
            if bops:
                str_aux += '_bops'
            item = root_fa.find(str_aux)
            out = []
            for i in item.findall('./string'):
                out.append(i.text)
            span.set_attribute("strings.recovered", out)
            return out

    def ingesta_diaria_aragon_por_tipo(self, dia, directorio_base, tipo, root_fc):
        with self.tracer.start_as_current_span(f"Ingesta diaria Aragón por tipo: {tipo}") as span:
            diaSemana = list(calendar.day_name)[datetime.strptime(dia, '%Y%m%d').weekday()]
            span.set_attribute("dia_semana", diaSemana)
            prefijo_url_html = "http://www.boa.aragon.es/cgi-bin/EBOA/BRSCGI?CMD=VERDOC&BASE=BZHT&DOCR="
            sufijo_url_html = "&SEC=BUSQUEDA_AVANZADA&SORT=-PUBL&SEPARADOR=&%40PUBL-GE=" + dia + "&%40PUBL-LE=" + dia + \
                "&NUMB=&RANG=&TITU-C=&FDIS-C=&TITU=&ORGA-C=&TEXT-C=&SECC-C=" + root_fc.find('./parametros_url/secc-c').text
            url_sumarizado = "http://www.boa.aragon.es/cgi-bin/EBOA/BRSCGI?CMD=VERLST&BASE=BZHT&DOCS=1-100&SEC=OPENDATABOAXML" + \
                "&OUTPUTMODE=XML&SORT=-PUBL&SEPARADOR=&%40PUBL-GE=" + dia + "&%40PUBL-LE=" + dia + \
                "&NUMB=&RANG=&TITU-C=&FDIS-C=&TITU=&ORGA-C=&TEXT-C=&SECC-C=" + root_fc.find('./parametros_url/secc-c').text

            #urls_sumarizados, sufijos_url_html = self.preparar_urls(tipo, root_fc, url_sumarizado, sufijo_url_html)

            urls_sumarizados = []
            sufijos_url_html = []
            if tipo == 'BOA':
                iter = root_fc.find('./parametros_url').findall('./info_seccion')
                if iter:									# Si en el FC hay secciones, coger el sumarizado de todas ellas. Si no, coger todo.
                    for s in iter:
                        secc = s.find('./seccion').text
                        subs = s.find('./subseccion').text
                        urls_sumarizados.append(url_sumarizado + '&SECC=' + secc + '&SUBS-C=' + subs + '&MATE-C=')
                        sufijos_url_html.append(sufijo_url_html + '&SECC=' + secc + '&SUBS-C=' + subs+ '&MATE-C=')
                else:
                    urls_sumarizados.append(url_sumarizado)
                    sufijos_url_html.append(sufijo_url_html)
            else:
                urls_sumarizados.append(url_sumarizado)
                sufijos_url_html.append(sufijo_url_html)

            strings_apertura = self.recuperar_strings('apertura')
            strings_cierre = self.recuperar_strings('cierre')
            strings_no_empleo = self.recuperar_strings('no_empleo', False)
            strings_apertura_bops = self.recuperar_strings('apertura', True)
            strings_cierre_bops = self.recuperar_strings('cierre', True)
            strings_no_empleo_bops = self.recuperar_strings('no_empleo', True)
            indice = 0

            for indice_iter in range(len(urls_sumarizados)):
                url_sumarizado = urls_sumarizados[indice_iter]
                sufijo_url_html = sufijos_url_html[indice_iter]

                session = requests.Session()
                information = session.get(url_sumarizado)
                encoded_information = information.content
                decoded_information = encoded_information.decode(root_fc.find('./charsets/url_sumarizado').text)

                # Chequear que en el día indicado ha habido boletín del tipo indicado.
                try:
                    root_check = ET.fromstring(decoded_information)
                except Exception as e:
                    msg = f"Failed obtention: No se ha recuperado un fichero XML. El dia {dia} es {diaSemana} y el tipo es {tipo}. Su URL es {url_sumarizado}"
                    self.logger.exception(msg)
                    span.record_exception(e)
                    span.set_status(trace.status.StatusCode.ERROR)
                    return

                try:
                    assert root_check.tag == root_fc.find('./etiquetas_xml_sumario/raiz').text
                except Exception as e:
                    msg = f"Failed assert: El tag del root es {root_check.tag}, el dia {dia} es {diaSemana} y el tipo es {tipo}."
                    self.logger.exception(msg)
                    span.record_exception(e)
                    span.set_status(trace.status.StatusCode.ERROR)
                    return

            with self.tracer.start_as_current_span("Guardar XML Sumarizado") as span:
                try:
                    if len(urls_sumarizados) > 1:
                        nombre_sumario = tipo + '_Sumarizado_' + dia + '_' + str(indice_iter+1) + '.xml'
                    else:
                        nombre_sumario = tipo + '_Sumarizado_' + dia + '.xml'
                    ruta_sumario = directorio_base / dia / nombre_sumario
                    with open(ruta_sumario, 'wb') as file:
                        file.write(encoded_information)
                    self.logger.info(f"XML sumarizado guardado en {ruta_sumario}")
                    span.set_attribute("file.path", str(ruta_sumario))
                except Exception as e:
                    msg = f"Failed: Write {url_sumarizado} content in {ruta_sumario}."
                    self.logger.exception(msg)
                    span.record_exception(e)
                    span.set_status(trace.status.StatusCode.ERROR)    
                    
            # Leer y parsear el fichero sumarizado
            try:
                with open(ruta_sumario, 'rb') as file:
                    tree = ET.parse(file)
                    root = tree.getroot()
            except:
                msg = (
                    "\nFailed: Open {ruta_sumario}"
                ).format(
                    ruta_sumario=ruta_sumario
                )
                logger.exception(
                    msg
                )
                return

            # Por cada artículo del XML sumarizado, crear un nuevo XML y obtener y guardar PDF y HTML.
            for item in root.findall(root_fc.find('./etiquetas_xml_sumario/registro').text):
                indice += 1
                # Realizar la división en aperturas y cierres. (Preferencia a aperturas en BOA, a cierres en BOPs por haber menos)
                tit = item.find(root_fc.find('./etiquetas_xml/auxiliares/titulo').text).text
                if tipo == 'BOA':
                    no_es_de_empleo = False
                    for cadena in strings_no_empleo:	# Si no es de empleo, no guardar artículo
                        if cadena in tit.lower():
                            no_es_de_empleo = True
                            break
                    if no_es_de_empleo:
                        continue
                    es_apertura = False
                    es_cierre = False
                    if 'por la que se deja sin efecto' in tit.lower():
                        tipo_articulo = 'cierre'
                    else:
                        for cadena in strings_apertura:
                            if cadena in tit.lower():
                                es_apertura = True
                                break
                        if es_apertura:			# Guardar en apertura
                            tipo_articulo = 'apertura'
                        else:
                            for cadena in strings_cierre:
                                if cadena in tit.lower():
                                    es_cierre = True
                                    break
                            if es_cierre:		# Guardar en cierre
                                tipo_articulo = 'cierre'
                            else:
                                continue		# Saltar el artículo
                else:
                    texto = item.find(root_fc.find('./etiquetas_xml/auxiliares/texto').text).text.lower()
                    inicio_texto = texto[:int(len(texto)/3)]
                    if self.encontrar_cadenas(tit.lower(), strings_no_empleo_bops) or self.encontrar_cadenas(inicio_texto.lower(), strings_no_empleo_bops) or \
                    self.encontrar_cadenas(tit.lower(), strings_no_empleo) or self.encontrar_cadenas(inicio_texto.lower(), strings_no_empleo) > 5:
                        # Evitar artículos que no sean ofertas de empleo
                        continue
                    num_encontrados = self.encontrar_cadenas(tit.lower(), strings_cierre_bops)
                    if num_encontrados:				# Si encuentra algún cierre en el título, guardar en cierre
                        tipo_articulo = 'cierre'
                    else:
                        num_encontrados = self.encontrar_cadenas(tit.lower(), strings_apertura_bops)
                        # Si encuentra más de un indicativo de empleo en el título o esa cadena en el texto, guardar en apertura
                        if num_encontrados > 1 or 'bases de la convocatoria' in texto:
                            tipo_articulo = 'apertura'
                        else:
                            inicio_texto = texto[:int(len(texto)/3)]
                            num_encontrados = self.encontrar_cadenas(inicio_texto, strings_cierre_bops)
                            if num_encontrados:					# Si encuentra algún cierre en el primer tercio, guardar en cierre
                                tipo_articulo = 'cierre'
                            else:
                                num_encontrados = self.encontrar_cadenas(texto, strings_apertura_bops) + self.encontrar_cadenas(tit, strings_apertura_bops)
                                if num_encontrados > 1:			# Si encuentra más de un indicativo de empleo en el texto, guardar en cierre
                                    tipo_articulo = 'apertura'
                                else:
                                    continue					# Si no, saltar el artículo

                # Guardar XML con registro como root.
                nombre_fichero = tipo + '_' + dia + '_' + str(indice)
                nombre_xml_fichero = nombre_fichero + '.xml'
                tree_articulo = ET.ElementTree(item)
                with open(directorio_base / dia / tipo_articulo / 'xml' / nombre_xml_fichero,'wb') as file:
                    tree_articulo.write(file)

                # Ubicar el enlace del pdf del artículo.
                # 	(Si sale entre etiquetas de enlace, quitarlas. Si salen varios, coger el primero únicamente.)
                urls = {}
                aux = item.find(root_fc.find('./etiquetas_xml/auxiliares/urlPdf').text).text
                if aux.startswith('<enlace>'):
                    aux = aux[8:].split('</enlace>')[0]
                urls['pdf'] = aux

                # Crear url del html
                urls['html'] = prefijo_url_html + str(indice) + sufijo_url_html

                # Guardar pdf y html
                for formato in urls.keys():
                    nombre_fichero_f = nombre_fichero + '.' + formato

                    try:
                        with open(directorio_base / dia / tipo_articulo / formato / nombre_fichero_f, 'wb') as file:
                            file.write(requests.get(urls[formato]).content)

                    except:
                        msg = (
                            "\nFailed: Write {path}"
                        ).format(
                            path=str(directorio_base / dia / tipo_articulo / formato / nombre_fichero_f)
                        )
                        logger.exception(
                            msg
                        )

                # Creación del fichero de info e inserción de los datos de que se dispone
                root_info = ET.Element('root')
                articulo = ET.Element('articulo')
                root_info.append(articulo)
                fuente_datos = ET.SubElement(articulo, 'fuente_datos')
                fuente_datos.text = tipo
                fecha_publicacion = ET.SubElement(articulo, 'fecha_publicacion')
                fecha_publicacion.text = dia[-2:] + '/' + dia[4:6] + '/' + dia[:4]
                enlace = ET.SubElement(articulo, 'enlace_convocatoria')
                enlace.text = urls['html']

                # Coger los datos que se van a guardar en info desde el xml, indicados en el fc.
                etiquetas = []
                for i in root_fc.find('./etiquetas_xml/a_guardar').iter():
                    etiquetas.append((i.tag, i.text))
                etiquetas = etiquetas[1:]

                for et_tag, et_text in etiquetas:
                    SE = ET.SubElement(articulo, et_tag)
                    el = item.find(et_text)
                    if et_tag == 'titulo':
                        SE.text = el.text.replace('\n',' ') if el is not None else '-'
                    elif et_tag == 'rango' or et_tag == 'organo_convocante':
                        if el is None or el.text is None or el.text == '':
                            SE.text = '-'
                        elif et_tag == 'rango':
                            SE.text = el.text.capitalize()
                        else:
                            SE.text = el.text.upper()
                    else:
                        SE.text = el.text if el is not None else '-'

                if 'uri_eli' not in [e[0] for e in etiquetas]:
                    SE = ET.SubElement(articulo, 'uri_eli')
                    SE.text = '-'

                tree_info = ET.ElementTree(root_info)

                with open(directorio_base / dia / tipo_articulo / 'info' / nombre_xml_fichero,'wb') as file:
                    tree_info.write(file)

    def preparar_urls(self, tipo, root_fc, url_sumarizado, sufijo_url_html):
        with self.tracer.start_as_current_span("Preparar URLs") as span:
            urls_sumarizados = []
            sufijos_url_html = []
            if tipo == 'BOA':
                iter = root_fc.find('./parametros_url').findall('./info_seccion')
                if iter:
                    for s in iter:
                        secc = s.find('./seccion').text
                        subs = s.find('./subseccion').text
                        urls_sumarizados.append(url_sumarizado + '&SECC=' + secc + '&SUBS-C=' + subs + '&MATE-C=')
                        sufijos_url_html.append(sufijo_url_html + '&SECC=' + secc + '&SUBS-C=' + subs + '&MATE-C=')
                else:
                    urls_sumarizados.append(url_sumarizado)
                    sufijos_url_html.append(sufijo_url_html)
            else:
                urls_sumarizados.append(url_sumarizado)
                sufijos_url_html.append(sufijo_url_html)
            span.set_attribute("urls_sumarizados", urls_sumarizados)
            span.set_attribute("sufijos_url_html", sufijos_url_html)
            return urls_sumarizados, sufijos_url_html

    def ingesta_diaria_aragon(self, dia, directorio_base):
        with self.tracer.start_as_current_span("Ingesta diaria Aragón") as span:
            boletines = ['BOA', 'BOPH', 'BOPZ', 'BOPT']
            for boletin in boletines:
                root_fc = self.recuperar_fichero_configuracion(boletin)
                if root_fc:
                    self.ingesta_diaria_aragon_por_tipo(dia, directorio_base, boletin, root_fc)