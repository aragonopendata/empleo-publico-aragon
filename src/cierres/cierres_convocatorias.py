# Nombre: cierres_convocatorias.py
# Autor: Oscar Potrony
# Fecha: 17/11/2020
# Descripción: Comprueba si el fichero pasado como parámetro cierra una oferta abierta, y en ese caso indica su cierre.
# Invocación:
#   python cierres_convocatorias directorio_base dia ruta_fichero_regex
# 		   ruta_fichero_aux db_name db_host db_port db_user db_password
# Ejemplo invocación:
#	python cierres_convocatorias.py .\data\ 20200202 C:\AragonOpenData\aragon-opendata\tools\ficheros_configuracion\regex.xml
#		C:\AragonOpenData\aragon-opendata\tools\ficheros_configuracion\auxiliar.xml empleo_publico_aragon localhost 5432 postgres Postgres1

import sys
import os
import logging
import pathlib
import re

from xml.etree import ElementTree as ET
import psycopg2
import spacy

import datetime
from time import strptime
import locale

logger = logging.getLogger('cierres_convocatorias')
logging.basicConfig()

# create console handler with a higher log level
ch = logging.StreamHandler(sys.stdout)
ch.setLevel(logging.DEBUG)
logger.addHandler(ch)

locale.setlocale(locale.LC_ALL, 'es_ES')

# Recibe el nombre del mes, y devuelve una cadena con el número de mes (rellenado de ceros).
def cambio_mes(mes_str, ruta_fichero_aux):
	try:
		with open(pathlib.Path(ruta_fichero_aux), 'rb') as file:
			tree_aux = ET.parse(file)
			root_aux = tree_aux.getroot()
	except:
		msg = (
			"\nFailed: Open {ruta_fichero_aux}"
		).format(
			ruta_fichero_aux=ruta_fichero_aux
		)
		logger.exception(
			msg
		)
		sys.exit()

	return root_aux.find('./correspondencias_meses/' + mes_str.lower()).text

def len_puesto(tupla):
		return (len(tupla[1]), tupla)

# Devuelve una lista con los puestos
def obtener_puestos(root):
	out = []
	aux = root.find('./articulo/puestos')
	for item in aux.iterfind('./puesto'):
		out.append(item.text.lower() if item.text != '-' else None)
	return out

# Identificar, mediante reglas, la fecha de disposición de la oferta que se cerraría.
def obtener_fecha_disposicion_oferta(txt_filepath, root_info,
									 ruta_fichero_regex, ruta_fichero_aux):
	# Recuperar fichero con las expresiones regulares
	try:
		with open(ruta_fichero_regex, 'rb') as file:
			tree_re = ET.parse(file)
			root_re = tree_re.getroot()
	except:
		msg = (
			"\nFailed: Open {ruta_fichero_regex}"
		).format(
			ruta_fichero_regex=ruta_fichero_regex
		)
		logger.exception(
			msg
		)
		sys.exit()

	# Recuperar regex
	regex_fecha_disposicion = root_re.find('./reglas/cierre/texto_fecha_disposicion').text

	# Leer texto
	try:
		with open(txt_filepath, 'r', encoding='utf-8') as file:
			texto = file.read()
	except:
		msg = (
			"\nFailed: Read {ruta}"
		).format(
			ruta=txt_filepath
		)
		logger.exception(
			msg
		)
		sys.exit()

	nlp = spacy.load("es_core_news_md")
	doc_texto = nlp(texto)
	
	# Encontrar match
	iter = re.finditer(regex_fecha_disposicion, doc_texto.text)
	matches = []
	for match in iter:
		start, end = match.span()
		span = doc_texto.char_span(start, end)
		if span is not None:
			matches.append(span.text)
	try:
		assert len(matches) > 1
	except:
		msg = (
			"\nFailed: Find fecha_disposicion in {ruta_txt}"
		).format(
			ruta_txt=txt_filepath
		)
		logger.exception(
			msg
		)
		return -1

	match = matches[1]

	# Post procesar la cadena encontrada
	split_match = match.split(' ')
	if split_match[0] == 'Resolución':
		ultimo = split_match[-1]
		if ultimo.isnumeric():
			anyo = ultimo.zfill(4)
			mes = split_match[-3]
			dia = split_match[-5].zfill(2)
		else:
			mes = ultimo.zfill(2)
			dia = split_match[-3].zfill(2)
			fpub = root_info.find('./articulo/fecha_publicacion').text
			anyo = int(fpub.split('/')[-1])
			fpub_str = strptime(fpub, "%d/%m/%Y")
			d_p = datetime.datetime(fpub_str[0],fpub_str[1],fpub_str[2])

			# No se sabe el año, por lo que se pondrá el pasado más cercano a la fecha de publicación
			anyo_descubierto = False
			while not anyo_descubierto and anyo > 1900:
				fdisp_str = strptime(dia + '/' + mes + '/' + str(anyo), "%d/%B/%Y")	# Se pasa el nombre del mes en español por el locale indicado.
				d_d = datetime.datetime(fdisp_str[0],fdisp_str[1],fdisp_str[2])
				
				# Si con este año la fecha de disposición es anterior a la fecha de publicación -> Se elige este año
				if (d_d-d_p).days < 0:
					anyo_descubierto = True
					anyo = str(anyo)
				else:
					anyo -= 1

	elif split_match[0] == 'Orden':
		anyo = split_match[1].split('/')[2].zfill(4)
		dia = split_match[3].zfill(2)
		mes = split_match[5]
		
	else:
		print('Encontrado algo que no es Orden ni Resolución.')

	if mes.isnumeric():
		mes = mes.zfill(2)
	else:
		mes = cambio_mes(mes, ruta_fichero_aux)

	return dia + '/' + mes + '/' + anyo

def comprobar_cierre(txt_filepath, info_filepath, ruta_fichero_regex,
					 ruta_fichero_aux, conn, cursor):
	# Identificar con info_filepath la fuente de datos y el órgano convocante.
	try:
		# Leer fichero de info
		with open(info_filepath, 'rb') as file:
			tree_info = ET.parse(file)
			root_info = tree_info.getroot()
	except:
		msg = (
			"\nFailed: Open {info_filepath}"
		).format(
			info_filepath=info_filepath
		)
		logger.exception(
			msg
		)
		sys.exit()

	tipo_boletin = root_info.find('./articulo/fuente_datos').text
	organo = root_info.find('./articulo/organo_convocante').text
	enlace_cierre = root_info.find('./articulo/enlace_convocatoria').text
	texto = ''

	# Obtener, del PostgreSQL, las ofertas abiertas cuya fuente, órgano convocante y fecha de disposición coincidan.
	# De estas ofertas, obtener los puestos ofertados en cada una de ellas (su denominación)
	fecha_disposicion = obtener_fecha_disposicion_oferta(
		txt_filepath, root_info, ruta_fichero_regex, ruta_fichero_aux)
	if fecha_disposicion == -1: return
	print(fecha_disposicion)

	id_fecha_disposicion = fecha_disposicion.split('/')
	id_fecha_disposicion.reverse()
	id_fecha_disposicion = ''.join(id_fecha_disposicion)

	print(f'Tipo de boletín: {tipo_boletin}')
	print(f'Órgano convocante: {organo}')
	print(f'Fecha de disposición: {id_fecha_disposicion}')

	query = ('SELECT oo.id, p.denominacion '
			 'FROM puesto p '
			 'INNER JOIN ( '
				'SELECT o.id, o.id_puesto '
			 	'FROM oferta o '
			 	'INNER JOIN ( '
		 	 		'SELECT c.id '
					'FROM convocatoria c '
					'WHERE c.fuente = %s '
						'AND c.organo_convocante = %s '
						'AND c.id_fecha_disposicion = %s '
					') AS cc '
			 	'ON o.id_convocatoria = cc.id '
			 	'WHERE o.estado = %s '
			 	') AS oo '
			 'ON p.id = oo.id_puesto;')
	cursor.execute(query, (tipo_boletin, organo, id_fecha_disposicion,
						   'Abierta'))
	ofertas = cursor.fetchall()
	print(ofertas)

	# Para buscar primero los puestos más largos
	ofertas.sort(key=len_puesto, reverse=True)

	cerrar_oferta = []
	for i in range(len(ofertas)):
		cerrar_oferta.append(False)

	# Obtener lista de puestos en las tablas.
	lista_puestos_tablas = obtener_puestos(root_info)

	# Buscar todos estos puestos en el texto del txt (si no se ha leído para la fecha de disposición)
	if texto == '':
		try:
			# Leer el texto
			with open(txt_filepath, 'r', encoding='utf-8') as file:
				texto = file.read()
		except:
			msg = (
				"\nFailed: Read {ruta}"
			).format(
				ruta=txt_filepath
			)
			logger.exception(
				msg
			)
			sys.exit()

	# Búsqueda en el texto
	for i, tupla in enumerate(ofertas):
		puesto = tupla[1]

		# Primero comprobar en la lista de puestos obtenido de las tablas.
		if puesto.lower() in lista_puestos_tablas:
			cerrar_oferta[i] = True
			lista_puestos_tablas.remove(puesto.lower())

		elif puesto.lower() in texto.lower():
			cerrar_oferta[i] = True
			# Eliminar primera ocurrencia del puesto (la que ha encontrado)
			# para que una misma cadena de texto no cierre más de una oferta
			texto = texto.replace(puesto, '', 1)

	# Se indica el cierre (o se cierra directamente aquí) de las ofertas cuyos puestos se han detectado.
	# (Se entiende que los puestos aparecidos en un artículo de cierre aparecen para cerrarse)
	for i, cerrar in enumerate(cerrar_oferta):
		print(i, cerrar)
		if cerrar:
			id_oferta = ofertas[i][0]
			# TODO: Cerrar la oferta cuyo id es id_oferta o meter en nueva lista y devolver lista (y cerrar en otro sitio)
			sql = ('UPDATE oferta '
				   'SET estado = %s, enlace_cierre = %s '
				   'WHERE id = %s;')
			cursor.execute(sql, ('Cerrada', enlace_cierre, str(id_oferta)))

	conn.commit()


def comprobar_cierres_directorio(directorio_base, dia, ruta_fichero_regex,
								 ruta_fichero_aux, conn):
	cursor = conn.cursor()
	for filename in os.listdir(directorio_base / dia / 'cierre' / 'txt'):
		if '_legible' not in filename:
			ruta_texto = directorio_base / dia / 'cierre' / 'txt' / filename
			ruta_info = directorio_base / dia / 'cierre' / 'info' / (filename.replace('.txt', '.xml'))
			comprobar_cierre(ruta_texto, ruta_info, ruta_fichero_regex,
							 ruta_fichero_aux, conn, cursor)
	
	cursor.close()


def main():
	if len(sys.argv) != 10:
		print('Numero de parametros incorrecto.')
		sys.exit()

	directorio_base = pathlib.Path(sys.argv[1])
	dia = sys.argv[2]
	ruta_fichero_regex = pathlib.Path(sys.argv[3])
	ruta_fichero_aux = pathlib.Path(sys.argv[4])
	
	# Parámetros conexión Postgres
	db_name = sys.argv[5]
	db_host = sys.argv[6]
	db_port = sys.argv[7]
	db_user = sys.argv[8]
	db_password = sys.argv[9]

	# Crear conexión Postgres
	conn = psycopg2.connect(dbname=db_name, user=db_user, password=db_password,
							host=db_host, port=db_port)

	comprobar_cierres_directorio(directorio_base, dia, ruta_fichero_regex,
								 ruta_fichero_aux, conn)
	
	conn.close()


if __name__ == "__main__":
	main()