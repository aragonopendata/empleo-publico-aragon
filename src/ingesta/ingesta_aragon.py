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

correspondencias_tipos = {'BOA': 'BOA%2Bo%2BDisposiciones%2Bo%2BPersonal%2Bo%2BAcuerdos%2Bo%2BJusticia%2Bo%2BAnuncios',
						  'BOPH': 'huesca',
						  'BOPZ': 'zaragoza',
						  'BOPT': 'teruel'}

def ingesta_diaria_aragon_por_tipo(dia, directorio_base, tipo):
	diaSemana = list(calendar.day_name)[datetime.strptime(dia, '%Y%m%d').weekday()]
	url_sumarizado = "http://www.boa.aragon.es/cgi-bin/EBOA/BRSCGI?CMD=VERLST&BASE=BZHT&DOCS=1-100&SEC=OPENDATABOAXML&OUTPUTMODE=XML&\
	SORT=-PUBL&SEPARADOR=&%40PUBL-GE=" + dia + "&%40PUBL-LE=" + dia + "&NUMB=&RANG=&TITU-C=&FDIS-C=&TITU=&\
	ORGA-C=&TEXT-C=&SECC-C=" + correspondencias_tipos[tipo]
	if tipo == 'BOA':
		url_sumarizado += "&SECC=Personal&SUBS-C=Oposiciones&MATE-C="

	prefijo_url_html = "http://www.boa.aragon.es/cgi-bin/EBOA/BRSCGI?CMD=VERDOC&BASE=BZHT&DOCR="
	sufijo_url_html = "&SEC=BUSQUEDA_AVANZADA&SORT=-PUBL&SEPARADOR=&%40PUBL-GE=" + dia + "&%40PUBL-LE=" + dia + "&NUMB=&RANG=&TITU-C=&\
	FDIS-C=&TITU=&ORGA-C=&TEXT-C=&SECC-C=" + correspondencias_tipos[tipo] + "&SECC=Personal&SUBS-C=Oposiciones&MATE-C="
	
	session = requests.Session()
	information = session.get(url_sumarizado)
	encoded_information = information.content
	decoded_information = encoded_information.decode('iso-8859-1')

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
		assert root_check.tag == 'documento'
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
		nombre_sumario = tipo + '_Sumarizado_' + dia + '.xml'
		ruta_sumario = directorio_base / nombre_sumario
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
	for indice, item in enumerate(root.findall('./registro')):

		# PARA BORRAR (SOLO TEMPORAL)
		# Comprobar malamente si el documento tiene que ver con empleo si no es BOA. Si no lo es, pasar de documento.
		if tipo != 'BOA':
			texto_aux = item.find('./texto').text
			# import pdb; pdb.set_trace()
			if 'vacante' not in texto_aux and 'plazas' not in texto_aux:
				continue


		# Guardar XML con registro como root.
		nombre_fichero = tipo + '_' + dia + '_' + str(indice+1)
		nombre_xml_fichero = nombre_fichero + '.xml'
		tree_articulo = ET.ElementTree(item)
		with open(directorio_base / dia / 'xml' / nombre_xml_fichero,'wb') as file:
			tree_articulo.write(file)

		# Ubicar el enlace del pdf del artículo.
		# 	(Si sale entre etiquetas de enlace, quitarlas. Si salen varios, coger el primero únicamente.)
		urls = {}
		aux = item.find('./url').text
		if aux.startswith('<enlace>'):
			aux = aux[8:].split('</enlace>')[0]
		urls['pdf'] = aux

		# Crear url del html
		urls['html'] = prefijo_url_html + str(indice+1) + sufijo_url_html

		# Guardar pdf y html
		for formato in urls.keys():
			nombre_fichero_f = nombre_fichero + '.' + formato

			try:
				with open(directorio_base / dia / formato / nombre_fichero_f, 'wb') as file:
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

		# Obtención de título y órgano convocante (no hay ELI URI)
		tit = item.find('./titulo').text
		departamento = item.find('./emisor').text
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

def ingesta_diaria_aragon(dia, directorio_base):
	ingesta_diaria_aragon_por_tipo(dia, directorio_base, 'BOA')
	ingesta_diaria_aragon_por_tipo(dia, directorio_base, 'BOPH')
	ingesta_diaria_aragon_por_tipo(dia, directorio_base, 'BOPZ')
	ingesta_diaria_aragon_por_tipo(dia, directorio_base, 'BOPT')

def main():
	if len(sys.argv) != 3:
		print('Numero de parametros incorrecto.')
		sys.exit()

	dia = sys.argv[1]
	directorio_base = Path(sys.argv[2])

	ingesta_diaria_aragon(dia, directorio_base)


if __name__ == "__main__":
	main()
