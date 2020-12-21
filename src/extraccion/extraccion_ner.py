# Nombre: extraccion_ner.py
# Autor: Oscar Potrony
# Fecha: 20/11/2020
# Descripción: Extrae entidades del texto indicado utilizando el modelo NER indicado.
# Invocación:
#	python extraccion_ner.py ruta_texto ruta_modelo
# Ejemplo invocación:
#	python extraccion_ner.py C:\prueba\20201001\txt\BOE_20201001_2.txt C:\AragonOpenData\aragon-opendata\models\modelo_20201112_50

import sys
import logging
import re

from xml.etree import ElementTree as ET
import pathlib
import spacy

logger = logging.getLogger('extraccion_ner')
logging.basicConfig()

# create console handler with a higher log level
ch = logging.StreamHandler(sys.stdout)
ch.setLevel(logging.DEBUG)
logger.addHandler(ch)
logger.disabled = True

# Evalúa el modelo, devolviendo las entidades obtenidas y el documento del texto.
def evaluate_model(
        test_text: str,
        model_path: str,
        ):
    # Testing the NER
    nlp = spacy.load(model_path)
    doc = nlp(test_text)

    entidades = []
    for ent in doc.ents:
        entidad = (ent.label_, ent.text)
        entidades.append(entidad)

    return entidades, doc

# Segmentar el texto teniendo en cuenta el número máximo de carácteres de cada texto devuelto,
# y cortándolos por el carácter corte
def segmentar(texto, max_length, corte):
	textos = []
	lineas = texto.split(corte)

	# Primero comprobar si hay algún texto_aux cuyo len sea mayor a max_length.
	# Para ellos, ahora son = segmentar(ese subtexto, max_length, ' ').
	if corte != ' ':
		lineas_aux = lineas
		lineas = []
		for i_l, linea in enumerate(lineas_aux):
			if len(linea) > max_length:
				for s in segmentar(linea, max_length, ' '):
					lineas.append(s)
			else:
				lineas.append(linea)
		del(lineas_aux)

	iter_l = iter(lineas)
	actual = next(iter_l)
	for siguiente in iter_l:
		if len(actual) + len(corte) + len(siguiente) > max_length:	# Si la siguiente palabra no cabe, meter lo que se tiene
			textos.append(actual)
			actual = siguiente
		else:
			actual += corte + siguiente								# Si cabe, incorporar a lo que se tiene
	textos.append(actual)
	return textos

def obtener_campos_ner(ruta_texto, ruta_modelo):
	# Obtener el texto de su ruta
	try:
		with open(ruta_texto, 'r', encoding='utf-8') as file:
			texto_completo = file.read()
	except:
		msg = (
			"\nFailed: Read {ruta}"
		).format(
			ruta=ruta_texto
		)
		logger.exception(
			msg
		)
		sys.exit()

	texto_completo = texto_completo.split('TEMARIO')[0]		# Se coge el texto solo hasta que se encuentra un TEMARIO

	# Segmentar el texto
	max_length = 3100
	corte = '. '
	segmentos = segmentar(texto_completo, max_length, corte)

	# Evaluar los segmentos
	entidades_encontradas = []			# Lista de tuplas (etiqueta, texto)
	for segmento in segmentos:
		ents = evaluate_model(segmento, ruta_modelo)[0]
		if ents:
			for ent in ents:
				entidades_encontradas.append(ent)

	# Post-procesamiento: Subescalas
	for i, (et, an) in enumerate(entidades_encontradas):
		if et == 'escala' and 'subescala' in an.lower():		# Si ha clasificado una subescala como escala, clasificarla como subescala
			entidades_encontradas[i] = ('subescala', an)

	# Pasar a diccionario de entidades
	dic_entidades = {}
	for et, an in entidades_encontradas:
		if et not in dic_entidades.keys():
			dic_entidades[et] = []
		
		dic_entidades[et].append(an)

	return dic_entidades


def main():

	if len(sys.argv) != 3:
		print('Numero de parametros incorrecto.')
		sys.exit()

	ruta_texto = pathlib.Path(sys.argv[1])
	ruta_modelo = pathlib.Path(sys.argv[2])

	entidades = obtener_campos_ner(ruta_texto, ruta_modelo)

	for e in entidades.items():
		for a in e[1]:
			print(e[0], '--', a)

if __name__ == "__main__":
	main()
