# Nombre: ingesta_extra.py
# Autor: Oscar Potrony
# Fecha: 18/11/2020
# Descripción: Trata de descargar y almacenar los artículos del boletín indicado de la sección indicada en el fichero de
#			   configuración del día indicadoen formato AAAAMMDD en subdirectorios del directorio_base, que ha de existir.
# Invocación:
#	python ingesta_extra.py dia_AAAAMMDD directorio_base tipo_boletin
# Ejemplo invocación:
#	python ingesta_extra.py 20201117 .\data\raw\ tipo_boletin

import sys
import requests
import logging
from pathlib import Path

from xml.etree import ElementTree as ET
from datetime import datetime
import calendar
import locale

logger = logging.getLogger('ingesta_extra')
logging.basicConfig()

# create console handler with a higher log level
ch = logging.StreamHandler(sys.stdout)
ch.setLevel(logging.DEBUG)
logger.addHandler(ch)

locale.setlocale(locale.LC_ALL, 'es_ES')

# Recuperar las strings de apertura/cierre del fichero auxiliar
def recuperar_strings(tipo):
	ruta_fcs = Path(__file__).parent.parent / 'ficheros_configuracion'
	ruta_fichero_aux = ruta_fcs / 'auxiliar.xml'
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

def ingesta_diaria_extra(dia, directorio_base, tipo_boletin):
	# Recuperar el fichero de configuración
	ruta_fcs = Path(__file__).parent.parent / 'ficheros_configuracion'
	ruta_fichero_conf = ruta_fcs / (tipo_boletin + '_conf.xml')
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
		sys.exit()

	diaSemana = list(calendar.day_name)[datetime.strptime(dia, '%Y%m%d').weekday()]
	
	# Ha de recibir un XML Sumario
	prefijo_url_sumario = root_fc.find('./prefijo_url_sumario').text
	url = prefijo_url_sumario + dia 								# Se ha decidido poner como en el BOE

	response = requests.get(url)
	contenido = response.content

	# Chequear que en el día indicado ha habido Boletín (si el fichero de configuración lo permite)
	root_check = ET.fromstring(contenido)
	raiz = root_fc.find('./etiquetas_xml_sumario/raiz')
	if raiz is not None:
		try:
			assert root_check.tag == raiz.text
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
		nombre_sumario = tipo_boletin + '_Sumario_' + dia + '.xml'
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

	# Preparar iterable de artículos en el XML sumario
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


def main():
	if len(sys.argv) != 4:
		print('Numero de parametros incorrecto.')
		sys.exit()

	dia = sys.argv[1]
	directorio_base = Path(sys.argv[2])
	tipo_boletin = sys.argv[3]

	ingesta_diaria_extra(dia, directorio_base, tipo_boletin)


if __name__ == "__main__":
	main()