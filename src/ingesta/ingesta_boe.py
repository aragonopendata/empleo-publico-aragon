# Nombre: ingesta_boe.py
# Autor: Oscar Potrony
# Fecha: 29/09/2020
# Descripción: Descarga y almacena los artículos del BOE de la sección indicada en el fichero de configuración del día indicado
#			   en formato AAAAMMDD en subdirectorios del directorio_base, que ha de existir.
# Invocación:
#	python ingesta_boe.py dia_AAAAMMDD directorio_base
# Ejemplo invocación:
#	python ingesta_boe.py 20200929 .\data\raw\

import sys
import requests
import logging
from pathlib import Path

from xml.etree import ElementTree as ET
from datetime import datetime
import calendar
import locale

from PyPDF2 import PdfFileReader, PdfFileWriter		# pip install PyPDF2

logger = logging.getLogger('ingesta_boe')
logging.basicConfig()

# create console handler with a higher log level
ch = logging.StreamHandler(sys.stdout)
ch.setLevel(logging.DEBUG)
logger.addHandler(ch)

locale.setlocale(locale.LC_ALL, 'es_ES')

# Recuperar las strings de apertura/cierre del fichero auxiliar
def recuperar_strings(tipo):
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

	item = root_fa.find('./strings_' + tipo)
	out = []
	for i in item.findall('./string'):
		out.append(i.text)

	return out

def rotar_pdf(path_in, path_out):
	try:
		pdf_in = open(path_in, 'rb')
		pdf_reader = PdfFileReader(pdf_in)
	except:
		msg = (
			"\nFailed: Read {ruta}"
		).format(
			ruta=path_in
		)
		logger.exception(
			msg
		)
		sys.exit()
	
	# Rotar 90º página por página e incorporar al PDF de salida
	pdf_writer = PdfFileWriter()
	for pagenum in range(pdf_reader.numPages):
			page = pdf_reader.getPage(pagenum)
			page.rotateClockwise(90)
			pdf_writer.addPage(page)

	try:
		pdf_out = open(path_out,'wb')
		pdf_writer.write(pdf_out)
		pdf_out.close()
	except:
		msg = (
			"\nFailed: Write {ruta}"
		).format(
			ruta=path_out
		)
		logger.exception(
			msg
		)
		sys.exit()


def ingesta_diaria_boe(dia, directorio_base):
	ruta_fichero_conf = Path('../ficheros_configuracion/BOE_conf.xml')
	try:
		with open(ruta_fichero_conf, 'rb') as file:
			tree_fc = ET.parse(file)
			root_fc = tree_fc.getroot()
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

	diaSemana = list(calendar.day_name)[datetime.strptime(dia, '%Y%m%d').weekday()]
	prefijo_url_sumario = root_fc.find('./prefijo_url_sumario').text
	url = prefijo_url_sumario + dia
	prefijo_url = root_fc.find('./prefijo_url').text

	# Obtener XML sumario
	response = requests.get(url)
	contenido = response.content

	# Chequear que en el día indicado ha habido BOE
	root_check = ET.fromstring(contenido)
	try:
		assert root_check.tag == root_fc.find('./etiquetas_xml/raiz').text
	except:
		msg = (
			"\nFailed assert: El tag del root es {tag},"
			" el dia {dia} es {diaSemana}"
		).format(
			tag=root_check.tag,
			dia=dia,
			diaSemana=diaSemana
		)
		logger.exception(
			msg
		)
		sys.exit()

	# Guardar XML sumario
	try:
		nombre_sumario = 'BOE_Sumario_' + dia + '.xml'
		ruta_sumario = directorio_base / dia / nombre_sumario
		with open(ruta_sumario,'wb') as file:
			file.write(contenido)
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
		sys.exit()

	# Leer y parsear fichero sumario
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
		sys.exit()

	strings_apertura = recuperar_strings('apertura')
	strings_cierre = recuperar_strings('cierre')
	indice = 1
	# Por cada artículo de las secciones que nos interesan del fichero de configuración:
	#		(se juntan ambas porque en la 2A también aparecen nombramientos)
	for item in root.findall(root_fc.find('./secciones_xml/oposiciones').text) + \
				root.findall(root_fc.find('./secciones_xml/nombramientos').text):

		# Comprobar si el fichero es de apertura o cierre (preferencia a apertura)
		tit = item.find(root_fc.find('./etiquetas_xml/titulo_item').text).text
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

		nombre_fichero = 'BOE_' + dia + '_' + str(indice)

		# Obtener y guardar ficheros de los distintos formatos indicados en el fichero de configuración.
		formatos = []
		for t in root_fc.find('url_formatos').iter():
			formatos.append(t.tag)
		formatos = formatos[1:]		# Quitar el propio tag de url_formatos
		if 'xml' in formatos:		# Poner el xml primero
			formatos.remove('xml')
			formatos.insert(0,'xml')

		siguiente_iteracion = False
		for formato in formatos:
			nombre_formato_fichero = nombre_fichero + '.' + formato
			ruta_fichero = directorio_base / dia / tipo_articulo / formato / nombre_formato_fichero
			url = prefijo_url + item.find('./' + root_fc.find('./url_formatos/' + formato).text).text
			try:
				# Comprobar el rango en el XML antes de guardar los ficheros
				contenido_url = requests.get(url).content
				if formato == 'xml':
					root_tmp = ET.fromstring(contenido_url)
					rango_encontrado = root_tmp.find(root_fc.find('./etiquetas_xml/rango').text).text
					if rango_encontrado.lower() not in ['resolución', 'resolucion', 'orden']:
						siguiente_iteracion = True	# No es convocatoria ni cierre de convocatoria, se descarta
						break
				with open(ruta_fichero, 'wb') as file:
					file.write(contenido_url)

			except:
				msg = (
					"\nFailed: Write {path}"
				).format(
					path=ruta_fichero
				)
				logger.exception(
					msg
				)

			# Guardar fichero txt con la URL del HTML
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

			# Lectura del XML del artículo para obtener título, órgano convocante, ELI URI, rango, id orden y f_disp
		nombre_xml_fichero = nombre_fichero + '.xml'
		with open(directorio_base / dia / tipo_articulo / 'xml' / nombre_xml_fichero,'rb') as file:
			tree_aux = ET.parse(file)
			root_aux = tree_aux.getroot()

			# Obtención de dichos seis campos
		tit = root_aux.find(root_fc.find('./etiquetas_xml/titulo').text).text
		departamento = root_aux.find(root_fc.find('./etiquetas_xml/organo').text).text
		eli = root_aux.find(root_fc.find('./etiquetas_xml/ELI').text)
		if eli is not None:
			eli = eli.text
		else:
			eli = '-'
		rango_encontrado = root_aux.find(root_fc.find('./etiquetas_xml/rango').text).text
		idor = '-'
		if rango_encontrado.lower() == 'orden':
			idor = root_aux.find(root_fc.find('./etiquetas_xml/id_orden').text).text
		f_disp = root_aux.find(root_fc.find('./etiquetas_xml/fecha_disposicion').text).text
		f_disp = f_disp[-2:]+'/'+f_disp[5:7]+'/'+f_disp[:4]										# Darle formato

		organo_convocante = ET.SubElement(articulo, 'organo_convocante')
		organo_convocante.text = departamento
		titulo = ET.SubElement(articulo, 'titulo')
		titulo.text = tit
		eli_uri = ET.SubElement(articulo, 'uri_eli')
		eli_uri.text = eli
		rango = ET.SubElement(articulo, 'rango')
		rango.text = rango_encontrado
		id_orden = ET.SubElement(articulo, 'id_orden')
		id_orden.text = idor
		fecha_disposicion = ET.SubElement(articulo, 'fecha_disposicion')
		fecha_disposicion.text = f_disp


		tree_info = ET.ElementTree(root_info)

		with open(directorio_base / dia / tipo_articulo / 'info' / nombre_xml_fichero,'wb') as file:
			tree_info.write(file)


		# Crear pdf rotado
		nombre_pdf = nombre_fichero + '.pdf'
		ruta_pdf = directorio_base / dia / tipo_articulo / 'pdf' / nombre_pdf
		ruta_pdf_rotado = directorio_base / dia / tipo_articulo / 'pdf' / 'rotados' / nombre_pdf
		rotar_pdf(ruta_pdf, ruta_pdf_rotado)

		indice += 1


def main():
	if len(sys.argv) != 3:
		print('Numero de parametros incorrecto.')
		sys.exit()

	dia = sys.argv[1]
	directorio_base = Path(sys.argv[2])

	ingesta_diaria_boe(dia, directorio_base)


if __name__ == "__main__":
	main()