# Nombre: ingesta.py
# Autor: Oscar Potrony
# Fecha: 02/10/2020
# Descripción: Descarga y almacena los artículos de los boletines de la sección indicada en el fichero de configuración del día indicado
#			   en formato AAAAMMDD en subdirectorios del directorio_base, que ha de existir (y no terminar en \).
# Invocación:
#	python ingesta.py dia_AAAAMMDD directorio_base
# Ejemplo invocación:
#	python ingesta.py 20200929 .\data\raw

import sys
import logging
import os

from pathlib import Path

import ingesta_boe
import ingesta_aragon
import configuracion_auxiliar as conf

logger = logging.getLogger('ingesta')
logging.basicConfig()

# create console handler with a higher log level
ch = logging.StreamHandler(sys.stdout)
ch.setLevel(logging.DEBUG)
logger.addHandler(ch)


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

	# Crear directorios de formato si no están creados
	try:
		for formato in conf.url_formatos['BOE'].keys():
			path = directorio_base / dia / formato
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
		path = directorio_base / dia / 'info/'
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

	# Realizar la ingesta del BOE
	try:
		ingesta_boe.ingesta_diaria_boe(dia, directorio_base)
	except:
		msg = ("\nFailed: ingesta_boe")
		logger.exception(msg)

	try:
		ingesta_aragon.ingesta_diaria_aragon(dia, directorio_base)
	except:
		msg = ("\nFailed: ingesta_aragon")
		logger.exception(msg)


def main():
	if len(sys.argv) != 3:
		print('Numero de parametros incorrecto.')
		sys.exit()

	dia = sys.argv[1]
	directorio_base = Path(sys.argv[2])

	ingesta_diaria(dia, directorio_base)

if __name__ == "__main__":
    main()