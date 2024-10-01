# Nombre: conversion_a_texto.py
# Autor: Oscar Potrony
# Fecha: 02/10/2020
# Descripción: Convierte los ficheros del directorio indicado (del formato indicado en el fichero de configuración) a texto y los almacena
#			   en la carpeta txt. Los convierte con formato legible en caso de que bool_legible sea True.
# Invocación:
#	python conversion_a_texto.py directorio_base ruta_auxiliar bool_legible
# Ejemplo invocación:
#	python conversion_a_texto.py .\data\raw\20200929\ C:\AragonOpenData\aragon-opendata\tools\ficheros_configuracion\auxiliar.xml False

import sys
import os
import logging

from pathlib import Path
from xml.etree import ElementTree as ET

import pdf_a_txt
import xml_a_txt
import html_a_txt

logger = logging.getLogger('conversion_a_texto')
logging.basicConfig()

# create console handler with a higher log level
ch = logging.StreamHandler(sys.stdout)
ch.setLevel(logging.DEBUG)
logger.addHandler(ch)
logger.disabled = True

def conversion_a_texto(directorio_base, ruta_auxiliar, legible=False):
	try:
		with open(ruta_auxiliar, 'rb') as file:
			tree_auxiliar = ET.parse(file)
			root_auxiliar = tree_auxiliar.getroot()
	except:
		msg = (
			"\nFailed: Open {ruta_auxiliar}"
		).format(
			ruta_auxiliar=ruta_auxiliar
		)
		logger.exception(
			msg
		)
		sys.exit()

	# Crear directorio de txt si no está creado
	try:
		for tipo_articulo in ['apertura', 'cierre']:
			path_txt = directorio_base / tipo_articulo / 'txt'
			if not path_txt.exists():
				path_txt.mkdir()
	except:
		msg = (
			"\nFailed: Create {path}"
		).format(
			path=path_txt
		)
		logger.exception(
			msg
		)

	legible_sufijo = ''
	if legible:
		legible_sufijo = '_legible'

	for tipo_articulo in ['apertura', 'cierre']:
		for filename in os.listdir(directorio_base / tipo_articulo / 'pdf'):
			if filename != 'rotados':
				tipo_boletin = filename.split('_')[0]
				formato = root_auxiliar.find('./formatos_por_defecto/' + tipo_boletin).text
				input_filepath = directorio_base / tipo_articulo / formato / (filename.split('.')[0] + '.' + formato)
				output_filepath = directorio_base / tipo_articulo / 'txt' / (filename.split('.')[0] + legible_sufijo + '.txt')

				try:
					if formato == 'pdf':
						pdf_a_txt.from_pdf_to_text(
							input_filepath, output_filepath, tipo_boletin,
							legible)
					elif formato == 'xml':
						xml_a_txt.from_xml_to_text(
							input_filepath, output_filepath, tipo_boletin,
							legible)
					elif formato == 'html':
						html_a_txt.from_html_to_text(
							input_filepath, output_filepath, tipo_boletin,
							legible)
				except:
					fichero_log = Path(__file__).absolute().parent.parent / 'articulos_erroneos.log'
					with open(fichero_log, 'a') as file:
						file.write(f'{filename.split(".")[0]} - CONVERSION\n')


def main():
	if len(sys.argv) == 2:
		directorio_base = Path(sys.argv[1])
		ruta_auxiliar = Path(__file__).absolute().parent.parent / 'ficheros_configuracion' / 'auxiliar.xml'
		conversion_a_texto(directorio_base, ruta_auxiliar, False)
	elif len(sys.argv) != 4:
		print('Numero de parametros incorrecto.')
		sys.exit()
	else:
		directorio_base = Path(sys.argv[1])
		ruta_auxiliar = Path(sys.argv[2])
		legible = sys.argv[3].lower() in ['true', 't', 'verdadero']
		conversion_a_texto(directorio_base, ruta_auxiliar, legible)

if __name__ == "__main__":
	main()
