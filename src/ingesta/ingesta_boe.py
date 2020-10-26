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

import configuracion_auxiliar as conf

logger = logging.getLogger('ingesta_boe')
logging.basicConfig()

# create console handler with a higher log level
ch = logging.StreamHandler(sys.stdout)
ch.setLevel(logging.DEBUG)
logger.addHandler(ch)

locale.setlocale(locale.LC_ALL, 'es_ES')


def ingesta_diaria_boe(dia, directorio_base):
	diaSemana = list(calendar.day_name)[datetime.strptime(dia, '%Y%m%d').weekday()]
	url = 'https://boe.es/diario_boe/xml.php?id=BOE-S-' + dia
	prefijo = 'https://boe.es'

	# Obtener XML sumario
	response = requests.get(url)
	contenido = response.content

	# Chequear que en el día indicado ha habido BOE
	root_check = ET.fromstring(contenido)
	try:
		assert root_check.tag == 'sumario'
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
		ruta_sumario = directorio_base / nombre_sumario
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


	# Por cada artículo de la sección que nos interesa del fichero de configuración:
	for indice, item in enumerate(root.findall(conf.ruta_etiquetas_articulos_oposiciones['BOE'])):
		nombre_fichero = 'BOE_' + dia + '_' + str(indice+1)

		# Obtener y guardar ficheros de los distintos formatos indicados en el fichero de configuración.
		for formato in conf.url_formatos['BOE'].items():
			nombre_formato_fichero = nombre_fichero + '.' + formato[0]
			ruta_fichero = directorio_base / dia / formato[0] / nombre_formato_fichero
			url = prefijo + item.find('./' + formato[1]).text
			try:
				with open(ruta_fichero, 'wb') as file:
					file.write(requests.get(url).content)

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
			if formato[0] == 'html':
				url_html = url

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

			# Lectura del XML del artículo para obtener título, órgano convocante y ELI URI
		nombre_xml_fichero = nombre_fichero + '.xml'
		with open(directorio_base / dia / 'xml' / nombre_xml_fichero,'rb') as file:
			tree_aux = ET.parse(file)
			root_aux = tree_aux.getroot()

			# Obtención de dichos tres campos
		tit = root_aux.find(conf.etiquetas_xml['BOE']['titulo']).text
		departamento = root_aux.find(conf.etiquetas_xml['BOE']['organo']).text
		eli = root_aux.find(conf.etiquetas_xml['BOE']['ELI'])
		if eli is not None:
			eli = eli.text
		else:
			eli = '-'

		organo_convocante = ET.SubElement(articulo, 'organo_convocante')
		organo_convocante.text = departamento
		titulo = ET.SubElement(articulo, 'titulo')
		titulo.text = tit
		eli_uri = ET.SubElement(articulo, 'uri_eli')
		eli_uri.text = eli

		tree_info = ET.ElementTree(root_info)

		with open(directorio_base / dia / 'info' / nombre_xml_fichero,'wb') as file:
			tree_info.write(file)

def main():
	if len(sys.argv) != 3:
		print('Numero de parametros incorrecto.')
		sys.exit()

	dia = sys.argv[1]
	directorio_base = Path(sys.argv[2])

	ingesta_diaria_boe(dia, directorio_base)


if __name__ == "__main__":
	main()