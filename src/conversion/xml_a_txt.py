# Nombre: xml_a_txt.py
# Autor: Oscar Potrony
# Fecha: 02/10/2020
# Descripción: Lee el boletín indicado en XML y lo guarda como txt en la ruta indicada.
# Invocación:
#	python xml_a_txt.py ruta_xml ruta_txt tipo_boletin bool_doccano
# Ejemplo invocación:
#	python xml_a_txt.py .\data\raw\20200930\xml\BOE_20200930_1.xml .\data\extracted\20200930\txt\BOE_20200930_1.txt BOE False

import sys
import logging
import re

from xml.etree import ElementTree as ET

import textwrap3

import configuracion_auxiliar as conf

logger = logging.getLogger('xml_a_txt')
logging.basicConfig()

# create console handler with a higher log level
ch = logging.StreamHandler(sys.stdout)
ch.setLevel(logging.DEBUG)
logger.addHandler(ch)

MAX_CHARS_PER_LINE = 500

def from_xml_to_text(input_filepath, output_filepath, tipo_boletin, doccano=False):

	if tipo_boletin not in ['BOE', 'BOA', 'BOPH', 'BOPZ', 'BOPT']:
		print('Warning: El tipo de boletín indicado no es uno de los predefinidos.')

	aragon = tipo_boletin in ['BOA', 'BOPH', 'BOPZ', 'BOPT']

	tipo_codificacion = 'utf-8'		# por defecto
	if aragon:
		tipo_codificacion = 'ISO-8859-1'

	try:
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
	if aragon:
		texto = root.find(conf.etiquetas_xml[tipo_boletin]['texto']).text
	else:
		texto = ''
		for parrafo in root.findall(conf.etiquetas_xml[tipo_boletin]['texto']):
			texto += parrafo.text + '\n'
	
	# Escribir fichero
	try:
		fp = open(output_filepath, 'w+', encoding='utf-8')
		if doccano:
			fp.write(textwrap3.fill(texto, width=MAX_CHARS_PER_LINE))
        
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

	input_filepath = sys.argv[1]
	output_filepath = sys.argv[2]
	tipo_boletin = sys.argv[3]
	doccano = sys.argv[4].lower() in ['true', 't', 'verdadero']

	from_xml_to_text(input_filepath, output_filepath, tipo_boletin, doccano)

if __name__ == "__main__":
	main()