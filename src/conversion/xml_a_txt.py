# Nombre: xml_a_txt.py
# Autor: Oscar Potrony
# Fecha: 02/10/2020
# Descripción: Lee el boletín indicado en XML y lo guarda como txt en la ruta indicada.
# Invocación:
#	python xml_a_txt.py ruta_xml ruta_txt tipo_boletin bool_legible
# Ejemplo invocación:
#	python xml_a_txt.py .\data\raw\20200930\xml\BOE_20200930_1.xml .\data\extracted\20200930\txt\BOE_20200930_1.txt BOE False

import sys
import logging
import re

from xml.etree import ElementTree as ET
import pathlib

import textwrap3

logger = logging.getLogger('xml_a_txt')
logging.basicConfig()

# create console handler with a higher log level
ch = logging.StreamHandler(sys.stdout)
ch.setLevel(logging.DEBUG)
logger.addHandler(ch)

MAX_CHARS_PER_LINE = 500

def from_xml_to_text(input_filepath, output_filepath, tipo_boletin, legible=False):
	# Recuperar fichero de configuración
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
		return

	# Abrir el XML
	try:
		# No se usa porque se abre de forma binaria (rb)
		# tipo_codificacion = root_fc.find('./charsets/xml').text
		with open(input_filepath, 'rb') as file:
			tree = ET.parse(file)
			root = tree.getroot()
	except:
		msg = (
			"\nFailed: Read {path}"
		).format(
			path=input_filepath
		)
		logger.exception(
			msg
		)
		sys.exit()

	# Obtener texto
	if tipo_boletin == 'BOE':
		texto = ''
		for parrafo in root.findall(root_fc.find('./etiquetas_xml/auxiliares/texto').text):
			texto += parrafo.text + '\n'
	else:
		texto = root.find(root_fc.find('./etiquetas_xml/auxiliares/texto').text).text
	
	# Escribir fichero
	try:
		fp = open(output_filepath, 'w+', encoding='utf-8')
		if legible:
			texto = textwrap3.fill(texto, width=MAX_CHARS_PER_LINE)
        
		fp.write(texto)
		fp.close()
	except Exception as e:
		msg = (
			"\nFailed: Write {path}"
		).format(
			path=output_filepath
		)
		logger.exception(
			msg
		)


def main():
	if len(sys.argv) != 5:
		print('Numero de parametros incorrecto.')
		sys.exit()

	input_filepath = pathlib.Path(sys.argv[1])
	output_filepath = pathlib.Path(sys.argv[2])
	tipo_boletin = sys.argv[3]
	legible = sys.argv[4].lower() in ['true', 't', 'verdadero']

	from_xml_to_text(input_filepath, output_filepath, tipo_boletin, legible)

if __name__ == "__main__":
	main()