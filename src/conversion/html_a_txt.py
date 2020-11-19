# Nombre: html_a_txt.py
# Autor: Oscar Potrony
# Fecha: 02/10/2020
# Descripción: Convierte los ficheros del directorio indicado (del formato indicado en el fichero de configuración) a texto y los almacena
#			   en la carpeta txt. Los convierte con formato apto para Doccano en caso de que bool_doccano sea True.
# Invocación:
#	python html_a_txt.py ruta_html ruta_txt tipo_boletin bool_doccano
# Ejemplo invocación:
#	python html_a_txt.py .\data\raw\20200930\html\BOE_20200930_1.html .\data\extracted\20200930\txt\BOE_20200930_1.txt BOE False

import sys
import os
import logging
import pathlib

import codecs
from bs4 import BeautifulSoup
import textwrap3
from xml.etree import ElementTree as ET

logger = logging.getLogger('html_a_txt')
logging.basicConfig()

# create console handler with a higher log level
ch = logging.StreamHandler(sys.stdout)
ch.setLevel(logging.DEBUG)
logger.addHandler(ch)

MAX_CHARS_PER_LINE = 500

def from_html_to_text(input_filepath, output_filepath, tipo_boletin, doccano):
	# Recuperar fichero de configuración
	ruta_fichero_conf = pathlib.Path('../ficheros_configuracion/' + tipo_boletin + '_conf.xml')
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

	try:
		with codecs.open(input_filepath,'r', root_fc.find('./charsets/html').text) as file:			# LO REALIZADO ES DEL BOA Y BOPs (COMPROBAR CORRECTO FUNCIONAMIENTO)
			html = file.read()
	except:
		msg = (
			"\nFailed: Open {input_filepath}"
		).format(
			input_filepath=input_filepath
		)
		logger.exception(
			msg
		)
		return

	# Para BOE y Aragón se coge el texto entre cadenas conocidas y para otros se cogería todo el texto.
	# En BOE, con esto se consigue coger las td fácilmente, además de los párrafos.
	# Para los de Aragón es más necesario por la dificultad de parsearlo (está bastante mal estructurado).
	soup = BeautifulSoup(html, 'html.parser')
	texto_html = soup.get_text()
	
	if tipo_boletin in ['BOA', 'BOPH', 'BOPZ', 'BOPT']:
		inicio_texto = 'Texto completo:'
		fin_texto = '\n\n\n\n\n\n\n\n\n\n'
	elif tipo_boletin == 'BOE':
		inicio_texto = 'TEXTO ORIGINAL'
		fin_texto = '\nsubir\n'
	
	if tipo_boletin in ['BOA', 'BOPH', 'BOPZ', 'BOPT', 'BOE']:
		texto_html = texto_html[texto_html.find(inicio_texto):][len(inicio_texto):]
		texto_html = texto_html[:texto_html.find(fin_texto)]

	# Quitar primeros saltos de línea
	num_saltos = 0
	i = 0
	while(i<len(texto_html)):
		if texto_html[i] == '\n' or texto_html[i] == ' ':
			num_saltos += 1
		else:
			break
		i += 1

	texto_html = texto_html[num_saltos:]

	# Escribir fichero
	try:
		fp = open(output_filepath, 'w+', encoding='utf-8')
		if doccano:
			texto_html = textwrap3.fill(texto_html, width=MAX_CHARS_PER_LINE)
        
		fp.write(texto_html)
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
	doccano = sys.argv[4].lower() in ['true', 't', 'verdadero']

	from_html_to_text(input_filepath, output_filepath, tipo_boletin, doccano)

if __name__ == "__main__":
	main()
