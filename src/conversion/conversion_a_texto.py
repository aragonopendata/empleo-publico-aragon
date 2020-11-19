# Nombre: conversion_a_texto.py
# Autor: Oscar Potrony
# Fecha: 02/10/2020
# Descripción: Convierte los ficheros del directorio indicado (del formato indicado en el fichero de configuración) a texto y los almacena
#			   en la carpeta txt. Los convierte con formato apto para Doccano en caso de que bool_doccano sea True.
# Invocación:
#	python conversion_a_texto.py directorio_base bool_doccano
# Ejemplo invocación:
#	python conversion_a_texto.py .\data\raw\20200929\ False

import sys
import os
import logging

from pathlib import Path

import pdf_a_txt
import xml_a_txt
# import html_a_txt

logger = logging.getLogger('conversion_a_texto')
logging.basicConfig()

# create console handler with a higher log level
ch = logging.StreamHandler(sys.stdout)
ch.setLevel(logging.DEBUG)
logger.addHandler(ch)

def conversion_a_texto(directorio_base, doccano=False):
	
	# Crear directorio de txt si no está creado
	try:
		path = Path(directorio_base + 'txt/')
		if not path.exists():
			path.mkdir()
	except:
		msg = (
			"\nFailed: Create {path}"
		).format(
			path=path
		)
		logger.exception(
			msg
		)

	# Se han analizado (documento de validación) y es preferible el pdf, pues en ocasiones aparecen
	# tablas sin bordes en el BOA (con denominación y grupo del puesto), que no aparece en XML y HTML
	formato = 'pdf'
	formato = formato.lower()

	assert formato in ['pdf', 'html', 'xml']

	doccano_sufijo = ''
	if doccano:
		doccano_sufijo = '_doccano'

	for filename in os.listdir(directorio_base + formato + '/'):
		if filename is not 'rotados':
			input_filepath = directorio_base + formato + '/' + filename
			output_filepath = directorio_base + 'txt/' + filename.split('.')[0] + doccano_sufijo + '.txt'
			tipo_boletin = filename.split('_')[0]

			if formato == 'pdf':
				pdf_a_txt.from_pdf_to_text(input_filepath, output_filepath, tipo_boletin, doccano)
			elif formato == 'xml':
				xml_a_txt.from_xml_to_text(input_filepath, output_filepath, tipo_boletin, doccano)
			elif formato == 'html':
				html_a_txt.from_html_to_text(input_filepath, output_filepath, tipo_boletin, doccano)
			

def main():
	if len(sys.argv) != 3:
		print('Numero de parametros incorrecto.')
		sys.exit()

	directorio_base = sys.argv[1]
	doccano = sys.argv[2].lower() in ['true', 't', 'verdadero']

	conversion_a_texto(directorio_base, doccano)

if __name__ == "__main__":
	main()
