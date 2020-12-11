# Nombre: puestos_tablas_anexos.py
# Autor: Oscar Potrony
# Fecha: 27/11/2020
# Descripción: 
# Invocación:
#	python puestos_tablas_anexos.py ruta_pdf índice_pagina_tabla_opcional
# Ejemplo invocación:
#	python puestos_tablas_anexos.py C:\prueba\20201001\pdf\BOE_20201001_2_rotado.pdf 		# Para obtenerlo de todo el documento
#	python puestos_tablas_anexos.py C:\prueba\20201001\pdf\BOE_20201001_2_rotado.pdf 15		# Para obtener solo de la tabla de pág. 16

import sys
import re
import logging

from pathlib import Path

import pdfplumber

logger = logging.getLogger('puestos_tablas_anexos')
logging.basicConfig()

# create console handler with a higher log level
ch = logging.StreamHandler(sys.stdout)
ch.setLevel(logging.DEBUG)
logger.addHandler(ch)
logger.disabled = True

# Comprueba si es la única celda no vacía de su fila.
def unica_celda(fila, index_puesto):
	es_unica = True
	for i, celda in enumerate(fila):
		if i != index_puesto and (celda_no_vacia(celda)):
			es_unica = False
			break
	return es_unica

# Comprueba si la celda está vacía.
def celda_no_vacia(celda):
	return celda and celda is not None

# Para comprobar, mediante la primera palabra, si es un centro directivo o no.
def es_un_puesto(texto, lista_no_puestos):
	return celda_no_vacia(texto) and \
		   texto.lower().split(' ')[0] not in lista_no_puestos and \
		   texto.rstrip(' \t\n.-').lstrip(' \t\n.-') != ''

# Para quitar centros directivos de la misma celda, que están por detrás para las siguientes celdas.
def quitar_no_puesto_cola(texto, lista_no_puestos):
	lineas = texto.split('\n')
	puesto = ''
	ha_aparecido_puesto = False
	for linea in lineas:
		if es_un_puesto(linea, lista_no_puestos):
			puesto += ' ' + linea
			ha_aparecido_puesto = True
		elif ha_aparecido_puesto:
			break

	return puesto



# Devuelve True si se ha detectado una tabla con una de las denominaciones en la cabecera
def hay_tabla_puestos(tabla, lista_denominaciones):
	if tabla is None: return False
	if indices(tabla, lista_denominaciones) == -1: return False
	return True

# Devuelve el índice de la denominación encontrada en la cabecera. Sino, -1.
def indices(tabla, lista_denominaciones):
	i_puesto = -1
	i_cabecera = -1

	for i, e in enumerate(tabla[0]):
		if e is not None:
			for denominacion in lista_denominaciones:		# Comprobar si hay alguna de las denominaciones en el nombre
				if denominacion in e.lower():
					i_puesto = i
					i_cabecera = 0
					break
			if i_cabecera == 0:
				break

	if i_puesto == -1 and len(tabla) > 1:										# En la primera fila no lo ha detectado
		for i, e in enumerate(tabla[1]):
			if e is not None:
				for denominacion in lista_denominaciones:
					if denominacion in e.lower():
						i_puesto = i
						i_cabecera = 1
						break
				if i_cabecera == 1:
					break
	return i_puesto, i_cabecera

# Devuelve una lista de puestos obtenido de la tabla pasada por argumento.
def obtener_puestos_tabla(tabla, lista_denominaciones, lista_no_puestos):
	i_puesto, i_cabecera = indices(tabla, lista_denominaciones)

	lista_posibles_puestos = []
	if i_puesto > -1:									# Si en la segunda tampoco, se entiende que no es tabla de puestos.
		hay_puesto_en_anterior = False
		index_ultimo_puesto = -1
		for i_fila, fila in enumerate(tabla):
			if i_fila > i_cabecera:
				texto_celda = fila[i_puesto]
				col_orden = fila[0]
				if col_orden is not None:				# Si y solo si hay Nº de orden en la primera columna se cogerá el puesto de la misma.
					col_orden = re.sub('[\n ]+', '', col_orden)
					if celda_no_vacia(texto_celda) and col_orden.isnumeric():	# Si hay texto en la celda del puesto de la fila actual
						# texto_celda = re.sub('[ ]+', ' ', quitar_no_puesto_cola(texto_celda, lista_no_puestos))
						texto_celda = re.sub('[\n ]+', ' ', texto_celda)
						if hay_puesto_en_anterior and unica_celda(fila, i_puesto):	# Si en el anterior hay puesto y es continuación, completar
							lista_posibles_puestos[index_ultimo_puesto] = lista_posibles_puestos[index_ultimo_puesto] + ' ' + texto_celda

						else:										# Si no hay puesto en el anterior o no es continuación, meter un nuevo puesto
							lista_posibles_puestos.append(texto_celda)
							index_ultimo_puesto += 1
							hay_puesto_en_anterior = True
					else: hay_puesto_en_anterior = False
				else: hay_puesto_en_anterior = False

	# # Guardar solo los que son puestos de verdad
	# lista_puestos = []
	# for puesto in lista_posibles_puestos:
	# 	if es_un_puesto(puesto, lista_no_puestos):
	# 		lista_puestos.append(puesto)
	lista_puestos = []
	for puesto in lista_posibles_puestos:
		if not puesto.isnumeric() and len(puesto) >= 4:
			lista_puestos.append(puesto)

	return lista_puestos

# Comprueba si hay alguna tabla con puestos en el documento.
def hay_tabla_puestos_documento(pdf_path, lista_denominaciones):
	try:
		pdf = pdfplumber.open(pdf_path)
	except:
		msg = (
			"\nFailed: Read {ruta}"
		).format(
			ruta=pdf_path
		)
		logger.exception(
			msg
		)
		sys.exit()

	for page in pdf.pages:
		if hay_tabla_puestos(page.extract_table(), lista_denominaciones): return True
	return False

# Devuelve una lista de puestos del documento cuyo path se pasa por argumento.
def obtener_puestos_tablas_documento(pdf_path, lista_denominaciones, lista_no_puestos):
	try:
		pdf = pdfplumber.open(pdf_path)
	except:
		msg = (
			"\nFailed: Read {ruta}"
		).format(
			ruta=pdf_path
		)
		logger.exception(
			msg
		)
		sys.exit()

	lista_puestos = []
	for page in pdf.pages:
		tabla = page.extract_table()
		if hay_tabla_puestos(tabla, lista_denominaciones):
			lista_puestos += obtener_puestos_tabla(tabla, lista_denominaciones, lista_no_puestos)
	return lista_puestos

def main():

	lista_no_puestos = ['dirección', 'direccion', 'subdirección', 'subdireccion', 'secretaría', 'subsecretaría',
						 'subsecretaria', 'unidad', 'agencia', 'fondo', 'división', 'division', 'ministerio', 's.g.',
						 'gerencia', 'gerencias', 'ger.', 'ger.terr.de', 'ger.terr.', 'ag.', 'ag.esp.seguridad',
						 's.g.coord.de', 'centro', 'gabinete', 'oficialía', 'd.g.']
	lista_denominaciones = ['puesto de trabajo', 'denominación del puesto', 'denominación']

	if len(sys.argv) == 2: # Todo el documento
		ruta_pdf = Path(sys.argv[1])
		lista_puestos = obtener_puestos_tablas_documento(ruta_pdf, lista_denominaciones, lista_no_puestos)
	elif len(sys.argv) == 3: # Solo la página indicada
		pdf = pdfplumber.open(Path(sys.argv[1]))
		tabla = pdf.pages[int(sys.argv[2])].extract_table()
		lista_puestos = obtener_puestos_tabla(tabla, lista_denominaciones, lista_no_puestos)
	else:
		print('Numero de parametros incorrecto.')
		sys.exit()

	print(len(lista_puestos))
	for puesto in lista_puestos:
		print(puesto)

if __name__ == "__main__":
    main()
