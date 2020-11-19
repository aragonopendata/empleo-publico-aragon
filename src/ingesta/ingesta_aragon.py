# Nombre: ingesta_aragon.py
# Autor: Oscar Potrony
# Fecha: 29/09/2020
# Descripción: Descarga y almacena los artículos del BOA y de los boletines provinciales de la sección indicada en el fichero de configuración
#			   del día indicado en formato AAAAMMDD en subdirectorios del directorio_base, que ha de existir.
# Invocación:
#	python ingesta_aragon.py dia_AAAAMMDD directorio_base
# Ejemplo invocación:
#	python ingesta_aragon.py 20200929 .\data\raw\

import sys
import requests
import logging
from pathlib import Path
import urllib.request

from xml.etree import ElementTree as ET
from datetime import datetime
import calendar
import locale

logger = logging.getLogger('ingesta_aragon')
logging.basicConfig()

# create console handler with a higher log level
ch = logging.StreamHandler(sys.stdout)
ch.setLevel(logging.DEBUG)
logger.addHandler(ch)

locale.setlocale(locale.LC_ALL, 'es_ES')

def encontrar_cadenas(texto, strings):
	contador = 0
	for string in strings:
		if string in texto:
			contador += 1
	return contador

# Devuelve el elemento root del fichero de configuración correspondiente a tipo_boletin
def recuperar_fichero_configuracion(tipo_boletin):
	ruta_fichero_conf = Path('../ficheros_configuracion/' + tipo_boletin + '_conf.xml')
	try:
		with open(ruta_fichero_conf, 'rb') as file:
			tree = ET.parse(file)
			root = tree.getroot()
	except:
		msg = (
			"\nFailed: Open {ruta_fichero_conf}"
		).format(
			ruta_fichero_conf=ruta_fichero_conf
		)
		logger.exception(
			msg
		)
		return
	return root

# Recuperar las strings de apertura/cierre del fichero auxiliar
def recuperar_strings(tipo, bops=False):
	ruta_fichero_aux = Path('../ficheros_configuracion/auxiliar.xml')
	try:
		with open(ruta_fichero_aux, 'rb') as file:
			tree_fa = ET.parse(file)
			root_fa = tree_fa.getroot()
	except:
		msg = (
			"\nFailed: Open {ruta_fichero_conf}"
		).format(
			ruta_fichero_conf=ruta_fichero_conf
		)
		logger.exception(
			msg
		)
		return

	str_aux = './strings_' + tipo
	if bops:
		str_aux += '_bops'
	item = root_fa.find(str_aux)
	out = []
	for i in item.findall('./string'):
		out.append(i.text)

	return out

def ingesta_diaria_aragon_por_tipo(dia, directorio_base, tipo, root_fc):
	diaSemana = list(calendar.day_name)[datetime.strptime(dia, '%Y%m%d').weekday()]
	prefijo_url_html = "http://www.boa.aragon.es/cgi-bin/EBOA/BRSCGI?CMD=VERDOC&BASE=BZHT&DOCR="
	sufijo_url_html = "&SEC=BUSQUEDA_AVANZADA&SORT=-PUBL&SEPARADOR=&%40PUBL-GE=" + dia + "&%40PUBL-LE=" + dia + "&NUMB=&RANG=&TITU-C=&\
	FDIS-C=&TITU=&ORGA-C=&TEXT-C=&SECC-C=" + root_fc.find('./parametros_url/secc-c').text
	url_sumarizado = "http://www.boa.aragon.es/cgi-bin/EBOA/BRSCGI?CMD=VERLST&BASE=BZHT&DOCS=1-100&SEC=OPENDATABOAXML&OUTPUTMODE=XML&\
	SORT=-PUBL&SEPARADOR=&%40PUBL-GE=" + dia + "&%40PUBL-LE=" + dia + "&NUMB=&RANG=&TITU-C=&FDIS-C=&TITU=&\
	ORGA-C=&TEXT-C=&SECC-C=" + root_fc.find('./parametros_url/secc-c').text
	
	# Preparar URLs si se quieren obtener de diferentes subsecciones
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

	strings_apertura = recuperar_strings('apertura')
	strings_cierre = recuperar_strings('cierre')
	strings_apertura_bops = recuperar_strings('apertura',True)
	strings_cierre_bops = recuperar_strings('cierre',True)
	indice = 1	# Número de secuencia dado al artículo
	
	for indice_iter in range(len(urls_sumarizados)):
		url_sumarizado = urls_sumarizados[indice_iter]
		sufijo_url_html = sufijos_url_html[indice_iter]

		session = requests.Session()
		information = session.get(url_sumarizado)
		encoded_information = information.content
		decoded_information = encoded_information.decode(root_fc.find('./charsets/url_sumarizado').text)

		#Chequear que en el día indicado ha habido boletín del tipo indicado.
		try:
			root_check = ET.fromstring(decoded_information)
		except:
			msg = (
				"\nFailed obtention: No se ha recuperado un fichero XML."
				" El dia {dia} es {diaSemana} y el tipo es {tipo}."
			).format(
				dia=dia,
				diaSemana=diaSemana,
				tipo=tipo
			)
			logger.exception(
				msg
			)
			return

		try:
			assert root_check.tag == root_fc.find('./etiquetas_xml_sumario/raiz').text
		except:
			msg = (
				"\nFailed assert: El tag del root es {tag},"
				" el dia {dia} es {diaSemana} y el tipo es {tipo}."
			).format(
				tag=root_check.tag,
				dia=dia,
				diaSemana=diaSemana,
				tipo=tipo
			)
			logger.exception(
				msg
			)
			return

		# Guardar XML Sumarizado
		try:
			if len(urls_sumarizados) > 1:
				nombre_sumario = tipo + '_Sumarizado_' + dia + '_' + str(indice_iter+1) + '.xml'
			else:
				nombre_sumario = tipo + '_Sumarizado_' + dia + '.xml'
			ruta_sumario = directorio_base / dia / nombre_sumario
			with open(ruta_sumario,'wb') as file:
				file.write(encoded_information)                # Poniendo texto_xml_sumarizado pide bytes
		except:
			msg = (
				"\nFailed: Write {url} content"
				" in {ruta_sumario}."
			).format(
				url=url,
				ruta_sumario=ruta_sumario
			)
			logger.exception(
				msg
			)
			return

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

			# Realizar la división en aperturas y cierres. (Preferencia a aperturas en BOA, a cierres en BOPs por haber menos)
			tit = item.find(root_fc.find('./etiquetas_xml/titulo').text).text
			if tipo == 'BOA':
				es_apertura = False
				es_cierre = False
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
				texto = item.find(root_fc.find('./etiquetas_xml/texto').text).text.lower()
				num_encontrados = encontrar_cadenas(tit.lower(), strings_cierre_bops)
				if num_encontrados:				# Si encuentra algún cierre en el título, guardar en cierre
					tipo_articulo = 'cierre'
				else:
					num_encontrados = encontrar_cadenas(tit.lower(), strings_apertura_bops)
					# Si encuentra más de un indicativo de empleo en el título o esa cadena en el texto, guardar en apertura
					if num_encontrados > 1 or 'bases de la convocatoria' in texto:
						tipo_articulo = 'apertura'
					else:
						inicio_texto = texto[int(len(texto)/3):]
						num_encontrados = encontrar_cadenas(inicio_texto, strings_cierre_bops)
						if num_encontrados:					# Si encuentra algún cierre en el primer tercio, guardar en cierre
							tipo_articulo = 'cierre'
						else:
							num_encontrados = encontrar_cadenas(texto, strings_apertura_bops)
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
			aux = item.find(root_fc.find('./etiquetas_xml/urlPdf').text).text
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
						path=ruta_fichero
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

			# Obtención de rango y órgano convocante (título cogido antes, y no hay ELI URI)
			departamento = item.find(root_fc.find('./etiquetas_xml/organo').text).text
			rango_encontrado = item.find(root_fc.find('./etiquetas_xml/rango').text).text
			eli = '-'

			organo_convocante = ET.SubElement(articulo, 'organo_convocante')
			organo_convocante.text = departamento
			titulo = ET.SubElement(articulo, 'titulo')
			titulo.text = tit
			eli_uri = ET.SubElement(articulo, 'uri_eli')
			eli_uri.text = eli
			rango = ET.SubElement(articulo, 'rango')
			rango.text = rango_encontrado

			tree_info = ET.ElementTree(root_info)

			with open(directorio_base / dia / tipo_articulo / 'info' / nombre_xml_fichero,'wb') as file:
				tree_info.write(file)

			indice += 1

def ingesta_diaria_aragon(dia, directorio_base):
	ingesta_diaria_aragon_por_tipo(dia, directorio_base, 'BOA', recuperar_fichero_configuracion('BOA'))
	ingesta_diaria_aragon_por_tipo(dia, directorio_base, 'BOPH', recuperar_fichero_configuracion('BOPH'))
	ingesta_diaria_aragon_por_tipo(dia, directorio_base, 'BOPZ', recuperar_fichero_configuracion('BOPZ'))
	ingesta_diaria_aragon_por_tipo(dia, directorio_base, 'BOPT', recuperar_fichero_configuracion('BOPT'))

def main():
	if len(sys.argv) != 3:
		print('Numero de parametros incorrecto.')
		sys.exit()

	dia = sys.argv[1]
	directorio_base = Path(sys.argv[2])

	ingesta_diaria_aragon(dia, directorio_base)


if __name__ == "__main__":
	main()
