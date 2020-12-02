# Nombre: extraccion_tablas.py
# Autor: Oscar Potrony
# Fecha: 20/11/2020
# Descripción: Evalúa los nuevos artículos cuyo día se pasa como parámetro.
# Invocación:
#	python extraccion_tablas.py ruta_pdf ruta_auxiliar
# Ejemplo invocación:
#	python extraccion_tablas.py \data\20201116\apertura\pdf\BOE_20201116_1.pdf C:\AragonOpenData\aragon-opendata\tools\ficheros_configuracion\auxiliar.xml

import sys
import os
import logging
import re

from xml.etree import ElementTree as ET
from pathlib import Path

import puestos_tablas_anexos

logger = logging.getLogger('extraccion_tablas')
logging.basicConfig()

# create console handler with a higher log level
ch = logging.StreamHandler(sys.stdout)
ch.setLevel(logging.DEBUG)
logger.addHandler(ch)

# Devuelve una lista con las strings del campo indicado del fichero auxiliar indicado.
def obtener_lista(ruta_aux, campo):
	try:
		with open(ruta_aux, 'rb') as file:
			tree = ET.parse(file)
			root = tree.getroot()
	except:
		msg = (
			"\nFailed: Read {ruta}"
		).format(
			ruta=ruta_aux
		)
		logger.exception(
			msg
		)
		sys.exit()
	
	item = root.find(campo)
	out = []
	for i in item.findall('./string'):
		out.append(i.text)
	return out

# Devuelve true si la lista pasada es una lista correcta con puestos.
def es_lista_correcta(lista):
	# Si hay muchos \n o muchos carácteres sueltos en alguno de los puestos: False
	for e in lista:
		count_un_caracter = 0
		for word in re.split(r'\W+', e):
			if len(word) == 1:
				count_un_caracter += 1
		if e.count('\n') > 5 or count_un_caracter > 5:
			return False
	return True

# Devuelve una lista de puestos elegida entre las dos pasadas (normal y rotada).
def elegir_lista(lista_n, lista_r):
	if lista_n and es_lista_correcta(lista_n):
		return lista_n
	elif lista_r and es_lista_correcta(lista_r):
		return lista_r
	else: return []

# Devuelve una lista con los puestos obtenidos en tablas en el documento del pdf indicado.
def obtener_puestos(ruta_pdf, ruta_auxiliar):

	# Obtener listas de strings
	lista_denominaciones = obtener_lista(ruta_auxiliar, 'strings_cabecera_denominaciones')
	lista_no_puestos = obtener_lista(ruta_auxiliar, 'strings_no_puestos_tablas')

	puestos_normal = []
	puestos_rotado = []

	# Si hay tablas horizontales de puestos, coger esas.
	if puestos_tablas_anexos.hay_tabla_puestos_documento(ruta_pdf, lista_denominaciones):
		puestos_normal = puestos_tablas_anexos.obtener_puestos_tablas_documento(ruta_pdf, lista_denominaciones, lista_no_puestos)
	
	# Si no, si hay verticales, coger esas.
	ruta_pdf = ruta_pdf.parent / 'rotados' / ruta_pdf.name
	if ruta_pdf.exists() and puestos_tablas_anexos.hay_tabla_puestos_documento(ruta_pdf, lista_denominaciones):
		puestos_rotado = puestos_tablas_anexos.obtener_puestos_tablas_documento(ruta_pdf, lista_denominaciones, lista_no_puestos)


	return elegir_lista(puestos_normal, puestos_rotado)
	

def main():

	if len(sys.argv) != 3:
		print('Numero de parametros incorrecto.')
		sys.exit()

	ruta_pdf = Path(sys.argv[1])
	ruta_aux = Path(sys.argv[2])

	puestos = obtener_puestos(ruta_pdf, ruta_aux)

	print(len(puestos))
	for p in puestos:
		print(p)

if __name__ == "__main__":
	main()
