# Nombre: almacenamiento.py
# Autor: Oscar Potrony / Eliot Díaz
# Fecha: 26/11/2020
# Descripción: Almacena los datos del día indicado en la Base de datos indicada
# Invocación:
#	python almacenamiento.py dia_aaaammdd directorio_base ruta_auxiliar
#		   PSQL_DB PSQL_HOST PSQL_PORT PSQL_USER PSQL_PASS
# Ejemplo invocación:
#	python almacenamiento.py 20201001 \data\ 
#		   C:\AragonOpenData\aragon-opendata\tools\ficheros_configuracion\auxiliar.xml
#		   empleo_publico_aragon localhost 5432 postgres Postgres1

import sys
import os
import logging
import psycopg2
import itertools

from xml.etree import ElementTree as ET
from pathlib import Path
from datetime import date, timedelta, datetime
from time import strptime


logger = logging.getLogger('almacenamiento')

# create console handler with a higher log level
ch = logging.StreamHandler(sys.stdout)
ch.setLevel(logging.DEBUG)
logger.addHandler(ch)
logger.disabled = True

cambio_diaSemana = {
	'monday': 'Lunes',
	'tuesday': 'Martes',
	'wednesday': 'Miércoles',
	'thursday': 'Jueves',
	'friday': 'Viernes',
	'saturday': 'Sábado',
	'sunday': 'Domingo'
}


# Devuelve el número de campos de lista que no son None
def num_campos_detectados(lista):
	return sum(e is not None for e in lista)

def insertar_fecha(cursor, fecha_textual):
	if fecha_textual is None:
		return

	id_fecha, dia, mes, año, dia_semana = obtener_att_fecha(fecha_textual)
	try:
		sql = ('INSERT INTO fecha(id, dia, mes, año, diasemana, textual) '
			   'VALUES(%s, %s, %s, %s, %s, %s) '
			   'ON CONFLICT DO NOTHING;')
		cursor.execute(sql, (id_fecha, dia, mes, año, dia_semana, fecha_textual))
	except psycopg2.DatabaseError:
		return

	return id_fecha

def hay_puesto(puesto, cursor):
	sql = 'SELECT id FROM puesto WHERE '
	partes = ['denominacion', 'cuerpo', 'escala', 'subescala']
	nuevo_puesto = []
	for i, parte in enumerate(partes):
		sql += parte
		if puesto[i] is None: sql += ' is null'
		else:
			sql += ' = %s'
			nuevo_puesto.append(puesto[i])
		if i < len(partes)-1: sql += ' AND '
	sql += ';'

	cursor.execute(sql, nuevo_puesto)
	id_recuperado = cursor.fetchone()
	return id_recuperado[0] if cursor.fetchone() is not None else False


def insertar_puestos(cursor, denominaciones, cuerpo, escala, subescala):
	puestos_iter = itertools.product(
		denominaciones, [cuerpo], [escala], [subescala]) 

	ids_puestos = []
	for puesto in puestos_iter:
		try:
			id_puesto_existente = hay_puesto(puesto, cursor)
			if not id_puesto_existente:
				sql = ('INSERT INTO puesto(denominacion, cuerpo, escala, subescala) '
					   'VALUES(%s, %s, %s, %s) '
					   'RETURNING id;')
				cursor.execute(sql, puesto)
				ids_puestos.append(cursor.fetchone()[0])
			else:
				ids_puestos.append(id_puesto_existente)
		except psycopg2.DatabaseError:
			continue

	return ids_puestos


def insertar_convocatoria(cursor, organo_convocante, titulo, uri_eli, enlace,
						  grupo, plazo, rango, id_orden, fuente, tipo, 
						  datos_contacto, num_plazas, id_fecha_publicacion, 
						  id_fecha_disposicion, id_fecha_inicio_presentacion,
						  id_fecha_fin_presentacion):
	try:
		sql = ('INSERT INTO convocatoria('
					'organo_convocante, titulo, URI_ELI, enlace, grupo, plazo, '
					'rango, id_orden, fuente, tipo, datos_contacto, '
					'num_plazas, id_fecha_publicacion, id_fecha_disposicion, '
					'id_fecha_inicio_presentacion, id_fecha_fin_presentacion) '
			   'VALUES(%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, '
			   		  '%s, %s) '
			   'RETURNING id;')
		cursor.execute(sql, (organo_convocante, titulo, uri_eli, enlace, grupo,
							 plazo, rango, id_orden, fuente, tipo,
							 datos_contacto, num_plazas, id_fecha_publicacion,
							 id_fecha_disposicion, id_fecha_inicio_presentacion,
							 id_fecha_fin_presentacion))
		id_convocatoria = cursor.fetchone()[0]
	except psycopg2.DatabaseError:
		return

	return id_convocatoria


def insertar_oferta(cursor, estado, enlace_cierre, id_convocatoria,
					ids_puestos_lista):
	ids_ofertas_lista = []
	for id_puesto in ids_puestos_lista:
		id_oferta = generar_id_oferta(cursor, id_convocatoria, id_puesto)

		try:
			query = ('SELECT id '
					 'FROM oferta '
					 'WHERE id LIKE %s '
					 'ORDER BY cast(SPLIT_PART(id, %s, 5) AS integer) DESC;')
			cursor.execute(query, (f'{id_oferta}_%', '_'))
			ultimo_id = cursor.fetchone()
		except psycopg2.DatabaseError:
			continue

		if ultimo_id is None:
			serial = 0
		else:
			serial = int(ultimo_id[0].split('_')[-1])
		
		id_oferta = f'{id_oferta}_{serial + 1}'

		try:
			sql = ('INSERT INTO oferta(id, enlace_cierre, estado, '
									  'id_convocatoria, id_puesto) '
				   'VALUES(%s, %s, %s, %s, %s);')
			cursor.execute(sql, (id_oferta, enlace_cierre, estado,
								 id_convocatoria, id_puesto))
		except psycopg2.DatabaseError:
			continue

		ids_ofertas_lista.append(id_oferta)

	return ids_ofertas_lista


def generar_id_oferta(cursor, id_convocatoria, id_puesto):
	query = ('SELECT c.fuente, c.organo_convocante, c.id_fecha_disposicion, p.denominacion '
			 'FROM convocatoria c, puesto p '
			 'WHERE c.id = %s AND p.id = %s')

	cursor.execute(query, (id_convocatoria, id_puesto))
	data = cursor.fetchone()
	data = list(data)
	data[2] = str(data[2])

	return '_'.join(data)


def eliminar_records(conn, cursor):
	sql = 'DELETE FROM oferta;'
	cursor.execute(sql)

	sql = 'DELETE FROM convocatoria;'
	cursor.execute(sql)

	sql = 'DELETE FROM puesto;'
	cursor.execute(sql)
	
	sql = 'DELETE FROM fecha;'
	cursor.execute(sql)

	conn.commit()


# Devuelve la raíz del fichero indicado
def obtener_root_fichero(ruta):
	try:
		with open(ruta, 'rb') as file:
			tree = ET.parse(file)
			root = tree.getroot()
	except:
		msg = ("\nFailed: Read {ruta}").format(ruta=ruta)
		logger.exception(msg)
		sys.exit()

	return root


# Devuelve id, dia, mes, año y diaSemana de la fecha pasada.
def obtener_att_fecha(fecha):
	split = fecha.split('/')
	dia, mes, año = (int(x) for x in split)
	d = date(año, mes, dia)
	diaSemana = cambio_diaSemana[d.strftime("%A").lower()]
	id = split[2] + split[1] + split[0]
	return id, dia, mes, año, diaSemana


# Devuelve el texto del campo
def obtener_campo(root, campo):
	out = root.find('./articulo/'+campo)
	if out is not None:
		out = out.text
	return out if out != '-' else None


# Devuelve una lista con los puestos
def obtener_puestos(root):
	out = []
	aux = root.find('./articulo/puestos')
	for item in aux.iterfind('./puesto'):
		out.append(item.text if item.text != '-' else None)
	return out


# Almacena los datos del fichero XML cuya raíz se ha pasado como parámetro.
def almacenar(root, root_auxiliar, conn, filename):
	num_min_campos = int(root_auxiliar.find('./num_min_campos').text)

	cursor = conn.cursor()

	## TABLA FECHA
	textual_pub = obtener_campo(root, 'fecha_publicacion')
	textual_disp = obtener_campo(root, 'fecha_disposicion')
	textual_ini = obtener_campo(root, 'fecha_inicio_presentacion')
	textual_fin = obtener_campo(root, 'fecha_fin_presentacion')

	## TABLA PUESTO
	denominaciones = obtener_puestos(root)
	cuerpo = obtener_campo(root, 'cuerpo')
	escala = obtener_campo(root, 'escala')
	subescala = obtener_campo(root, 'subescala')

	## TABLA CONVOCATORIA
	fuente_datos = obtener_campo(root, 'fuente_datos')
	organo_convocante = obtener_campo(root, 'organo_convocante')
	enlace_convocatoria = obtener_campo(root, 'enlace_convocatoria')
	titulo = obtener_campo(root, 'titulo')
	uri_eli = obtener_campo(root, 'uri_eli')
	rango = obtener_campo(root, 'rango')
	id_orden = obtener_campo(root, 'id_orden')
	grupo = obtener_campo(root, 'grupo')
	tipo_convocatoria = obtener_campo(root, 'tipo_convocatoria')
	datos_contacto = obtener_campo(root, 'datos_contacto')
	plazo = obtener_campo(root, 'plazo')
	num_plazas = obtener_campo(root, 'num_plazas')

	## TABLA OFERTA
	estado = 'Abierta'
	enlace_cierre = None

	# Solo guardar en la Base de datos si en el fichero de info se ha encontrado un nº mínimo de campos.
	campos_detectados = num_campos_detectados([
		textual_pub, textual_disp, textual_ini, textual_fin, denominaciones,
		cuerpo, escala, subescala, fuente_datos, organo_convocante,
		enlace_convocatoria, titulo, uri_eli, rango, id_orden, grupo,
		tipo_convocatoria, datos_contacto, plazo, num_plazas
	])
	
	if campos_detectados >= num_min_campos:
		# Inserción en la base de datos.
		id_fecha_pub = insertar_fecha(cursor, textual_pub)
		id_fecha_disp = insertar_fecha(cursor, textual_disp)
		id_fecha_ini = insertar_fecha(cursor, textual_ini)
		id_fecha_fin = insertar_fecha(cursor, textual_fin)
		
		# Si no hay puesto, usar la escala como puesto.
		if denominaciones is None or not denominaciones:
			if escala is not None:
				denominaciones = [escala]
			elif cuerpo is not None:
				denominaciones = [cuerpo]
		elif denominaciones[0] is None:
			if escala is not None:
				denominaciones = [escala]
			elif cuerpo is not None:
				denominaciones = [cuerpo]

		ids_puestos = insertar_puestos(
			cursor, denominaciones, cuerpo, escala, subescala)
		
		id_convocatoria = insertar_convocatoria(
			cursor, organo_convocante, titulo, uri_eli, enlace_convocatoria,
			grupo, plazo, rango, id_orden, fuente_datos, tipo_convocatoria,
			datos_contacto, num_plazas, id_fecha_pub, id_fecha_disp,
			id_fecha_ini, id_fecha_fin)

		insertar_oferta(cursor, estado, enlace_cierre, id_convocatoria,
						ids_puestos)
	else:
		nombre_articulo = filename.split('.')[0]
		fecha_publicacion = datetime.strptime(textual_pub, '%d/%m/%Y')

		with open('articulos_no_insertados.log','a') as file:
			file.write(f'{nombre_articulo}\n')

		try:
			sql = ('INSERT INTO articulos_no_insertados(nombre, fecha, enlace, '
						'num_campos_detectados) '
				   'VALUES(%s, %s, %s, %s);')
			cursor.execute(sql, (nombre_articulo, fecha_publicacion,
								 enlace_convocatoria, campos_detectados))
		except psycopg2.DatabaseError:
			pass

	conn.commit()
	cursor.close()

# Almacenar lo del fichero indicado según el caso indicado.
def almacenar_pruebas_aceptacion(caso):
	ruta_auxiliar = Path(r'C:\AragonOpenData\aragon-opendata\tools\ficheros_configuracion\auxiliar.xml')
	ruta_casos = Path(r'C:\Users\opotrony\Desktop\Artículos de casos de prueba')

	cwd = ruta_casos / ('Caso_' + caso)
	for file in os.listdir(cwd):
		if '.xml' in file and 'copia' not in file:
			ruta_info = cwd / file

	conn = psycopg2.connect(dbname='empleo_publico_aragon', user='postgres', password='Postgres1',
								host='localhost', port=5432)
	almacenar(obtener_root_fichero(ruta_info), obtener_root_fichero(ruta_auxiliar), conn, file.split('.')[0])
	conn.close()

# Almacena los datos de todos los ficheros de info del directorio y día pasados.
def almacenar_todos(dia, directorio_base, ruta_auxiliar, conn):
	for filename in os.listdir(directorio_base / dia / 'apertura' / 'info'):
		ruta_info = directorio_base / dia / 'apertura' / 'info' / filename
		try:
			almacenar(obtener_root_fichero(ruta_info),
					  obtener_root_fichero(ruta_auxiliar), conn, filename)
		except:
			fichero_log = Path(__file__).absolute().parent.parent / 'articulos_erroneos.log'
			with open(fichero_log, 'a') as file:
				file.write(f'{filename.split(".")[0]} - ALMACENAMIENTO\n')


def main():
	if len(sys.argv) == 2:
		almacenar_pruebas_aceptacion(sys.argv[1])
	elif len(sys.argv) != 9:
		print('Numero de parametros incorrecto.')
	else:
		dia = sys.argv[1]
		directorio_base = Path(sys.argv[2])
		ruta_auxiliar = Path(sys.argv[3])

		# Parámetros conexión Postgres
		PSQL_DB = sys.argv[4]
		PSQL_HOST = sys.argv[5]
		PSQL_PORT = sys.argv[6]
		PSQL_USER = sys.argv[7]
		PSQL_PASS = sys.argv[8]

		# Crear conexión Postgres
		conn = psycopg2.connect(
			dbname=PSQL_DB, user=PSQL_USER, password=PSQL_PASS, host=PSQL_HOST,
			port=PSQL_PORT)

		almacenar_todos(dia, directorio_base, ruta_auxiliar, conn)
		conn.close()


if __name__ == "__main__":
	main()
