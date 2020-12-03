# Nombre: ingesta.py
# Autor: Oscar Potrony
# Fecha: 02/10/2020
# Descripción: Descarga y almacena los artículos de los boletines de la sección indicada en el fichero de configuración del
#			   día indicado en formato AAAAMMDD en subdirectorios del directorio_base, que ha de existir (y no terminar en \).
# Invocación:
#	python ingesta.py dia_AAAAMMDD directorio_base
# Ejemplo invocación:
#	python ingesta.py 20200929 .\data\raw

import sys
import logging
import os

from pathlib import Path
from xml.etree import ElementTree as ET

import ingesta_boe
import ingesta_aragon
import ingesta_extra

logger = logging.getLogger('ingesta')
logging.basicConfig()

# create console handler with a higher log level
ch = logging.StreamHandler(sys.stdout)
ch.setLevel(logging.DEBUG)
logger.addHandler(ch)

# Devuelve el elemento root del fichero de configuración correspondiente a tipo_boletin
def recuperar_fichero_configuracion(ruta_fcs, tipo_boletin):
	ruta_fichero_conf = ruta_fcs / (tipo_boletin + '_conf.xml')
	try:
		with open(ruta_fichero_conf, 'rb') as file:
			tree = ET.parse(file)
			root = tree.getroot()
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
	return root

def ingesta_diaria(dia, directorio_base):

	# Crear directorio del día si no está creado
	try:
		path = directorio_base / dia
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

	# Saber que formatos, mediante el fichero de configuración del BOE
	ruta_fcs = Path(__file__).parent.parent / 'ficheros_configuracion'
	ruta_fichero_conf = ruta_fcs / 'BOE_conf.xml'
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
	formatos = []
	for t in root_fc.find('url_formatos').iter():
		formatos.append(t.tag)
	formatos = formatos[1:]		# Quitar el propio tag de url_formatos

	# Crear directorios de formato si no están creados
	for tipo_articulo in ['apertura', 'cierre']:
		# Crear directorios de tipos si no están creados
		try:
			path = directorio_base / dia / tipo_articulo
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
		
		try:
			for formato in formatos:
				path = directorio_base / dia / tipo_articulo / formato
				if not path.exists():
					path.mkdir()
				if formato.lower() == 'pdf':	# Crear directorio de pdfs rotados
					path = directorio_base / dia / tipo_articulo / formato / 'rotados'
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

		# Crear directorio de info si no está creado
		try:
			path = directorio_base / dia / tipo_articulo / 'info/'
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

	# Comprobar de qué boletines se tienen ficheros de configuración
	boletines = []
	for x in os.listdir(ruta_fcs):
		if x.endswith('_conf.xml'):
			boletines.append(x.split('_')[0])

	for tipo_boletin in boletines:
		if tipo_boletin == 'BOE':								# Realizar la ingesta del BOE
			try:
				ingesta_boe.ingesta_diaria_boe(dia, directorio_base)
			except:
				msg = ("\nFailed: ingesta_boe")
				logger.exception(msg)
		elif tipo_boletin in ['BOA', 'BOPH', 'BOPZ', 'BOPT']:	# Realizar la ingesta de boletines provinciales aragoneses
			try:
				ingesta_aragon.ingesta_diaria_aragon_por_tipo(dia, directorio_base, tipo_boletin, recuperar_fichero_configuracion(ruta_fcs, tipo_boletin))
			except:
				msg = ("\nFailed: ingesta_" + tipo_boletin)
				logger.exception(msg)
		else:
			ingesta_extra.ingesta_diaria_extra(dia, directorio_base, tipo_boletin)


def main():
	if len(sys.argv) != 3:
		print('Numero de parametros incorrecto.')
		sys.exit()

	dia = sys.argv[1]
	directorio_base = Path(sys.argv[2])

	ingesta_diaria(dia, directorio_base)

if __name__ == "__main__":
    main()