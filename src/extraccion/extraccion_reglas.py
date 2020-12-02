# Nombre: extraccion_reglas.py
# Autor: Oscar Potrony
# Fecha: 20/11/2020
# Descripción: Extrae entidades del título y texto indicados utilizando las expresiones regulares del fichero auxiliar.
# Invocación:
#	python extraccion_reglas.py dia_aaaammdd ruta_info ruta_texto ruta_fichero_regex
# Ejemplo invocación:
#	python extraccion_reglas.py 20201001 C:\prueba\20201001\info\BOE_20201001_2.xml C:\prueba\20201001\txt\BOE_20201001_2.txt 
#		C:\AragonOpenData\aragon-opendata\tools\ficheros_configuracion\regex.xml

import sys
import logging
import re

from xml.etree import ElementTree as ET
import pathlib
import spacy

logger = logging.getLogger('rule_matcher')
logging.basicConfig()

# create console handler with a higher log level
ch = logging.StreamHandler(sys.stdout)
ch.setLevel(logging.DEBUG)
logger.addHandler(ch)

# Concatena los términos de la lista para utilizarlos con un '|' en una regex
def terminos_a_regex(lista_terminos):
	out = ''
	for termino in lista_terminos:
		out += termino + '|'
	return out[:-1]

# Devuelve los matches del documento con la regex pasada.
def encontrar_matches(regex, documento, ignore_case=True):
	if ignore_case:
		iter = re.finditer(regex, documento.text, flags=re.IGNORECASE)
	else:
		iter = re.finditer(regex, documento.text)
	
	matches = []
	for match in iter:
		start, end = match.span()
		span = documento.text[start:end]
		matches.append(span)

		# Anteriormente estaba como las línea que siguen. Cambiado porque en  
		# ocasiones el span no coincide con los tokens y en ese caso devolvía None.
		# span = documento.char_span(start, end)	
		# if span is not None:
		# 	matches.append(span.text)
	return matches

# Encuentra matches de plazo, eliminando los mal recogidos
def encontrar_matches_plazo(regex, doc):
	encontrados = [e.lower() for e in encontrar_matches(regex, doc)]
	if not encontrados: return encontrados
	terminos_eliminatorios = ['obtenido', 'obtenida', 'estas', 'estos', 'esta', 'este', 'de','los','las','en','varios','varias']
	out = []
	for e in encontrados:
		aux = e.split(' ')[0]
		if aux not in terminos_eliminatorios:
			out.append(e)

	return out

# Devuelve la raíz del fichero indicado
def obtener_root_fichero(ruta):
	try:
		with open(ruta, 'rb') as file:
			tree = ET.parse(file)
			root = tree.getroot()
	except:
		msg = (
			"\nFailed: Read {ruta}"
		).format(
			ruta=ruta
		)
		logger.exception(
			msg
		)
		sys.exit()
	return root

# Devuelve la lista de términos del tipo tipo aparecidos en el fichero de regex
def obtener_terminos_tipo(root, tipo):
	terminos = []
	for t in root.find('./terminos_tipo/' + tipo).findall('./termino'):
		terminos.append(t.text)
	return terminos

# Lee la expresión regular indicada del fichero auxiliar indicado.
def leer_regla(root, regla):
	return root.find('./reglas/apertura/' + regla).text

# Lee la expresión regular del plazo del fichero auxiliar indicado.
def leer_regla_plazo(root):
	return root.find('./reglas/apertura/plazo/num_dias').text + \
		   root.find('./reglas/apertura/plazo/tipo_dias').text + \
		   root.find('./reglas/apertura/plazo/contexto_1').text + \
		   root.find('./reglas/apertura/plazo/dia_inicio').text + \
		   root.find('./reglas/apertura/plazo/contexto_2').text

def obtener_campos_reglas(dia, ruta_info, ruta_texto, ruta_regex):

	# Leer el título del fichero de información
	root_info = obtener_root_fichero(ruta_info)
	titulo = root_info.find('./articulo/titulo').text
	hay_id_orden = root_info.find('./articulo/id_orden') is not None
	hay_f_disp = root_info.find('./articulo/fecha_disposicion') is not None
	rango = root_info.find('./articulo/rango')
	hay_rango = rango is not None and rango.text is not None and rango. text != '-'
	if hay_rango:
		rango = rango.text.replace('ó', 'o').replace('Ó','O')

	# Leer el texto.
	try:
		with open(ruta_texto, 'r', encoding='utf-8') as file:
			texto = file.read()
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

	# Adaptar textos al modelo de noticias español
	nlp = spacy.load("es_core_news_md")
	# Si no funciona, probar:
		# import es_core_news_md
		# nlp = es_core_news_md.load()
	doc_titulo = nlp(titulo)
	doc_texto = nlp(texto)

	# Leer términos de tipo convocatoria
	root_regex = obtener_root_fichero(ruta_regex)
	terminos_ambas = obtener_terminos_tipo(root_regex, 'libre_e_interna')
	terminos_libre = obtener_terminos_tipo(root_regex, 'libre')
	terminos_interna = obtener_terminos_tipo(root_regex, 'interna')

	# Leer expresiones regulares
	campos_a_extraer = ['cuerpo', 'escala', 'subescala', 'grupo', 'web', 'email', 'plazo', 'tipo_convocatoria', 'puesto', 'num_plazas']
	if not hay_f_disp:
		campos_a_extraer.append('fecha_disposicion')
	if not hay_id_orden and hay_rango and rango.lower() == 'orden':
		campos_a_extraer.append('id_orden')

	dic_regex = {}
	for campo in campos_a_extraer:
		if campo == 'plazo':
			dic_regex[campo] = leer_regla_plazo(root_regex)
		elif campo == 'tipo_convocatoria':
			dic_regex[campo] = r"(" + terminos_a_regex(terminos_ambas + terminos_libre + terminos_interna) + r")"
		elif campo == 'fecha_disposicion':
			if hay_rango:
				dic_regex[campo] = leer_regla(root_regex, campo+'/'+rango.lower())
			else:
				dic_regex[campo] = leer_regla(root_regex, campo+'/resolucion')
		elif campo == 'puesto':
			dic_regex[campo] = {}
			for tipo_puesto in ['denominacion', 'cuerpo_escala', 'especialidad', 'identificacion']:
				dic_regex[campo][tipo_puesto] = leer_regla(root_regex, campo+'/'+tipo_puesto)
		else:
			dic_regex[campo] = leer_regla(root_regex, campo)

	# Encontrar matches
	dic_entidades = {}
	for campo in campos_a_extraer:
		if campo in ['tipo_convocatoria', 'num_plazas']:
			encontrado_ti = encontrar_matches_plazo(dic_regex[campo], doc_titulo)
			encontrado_te = encontrar_matches_plazo(dic_regex[campo], doc_texto)
			if encontrado_ti:
				dic_entidades[campo+'_titulo'] = encontrado_ti
			if encontrado_te:
				dic_entidades[campo+'_texto'] = encontrado_te
		elif campo == 'fecha_disposicion' or campo == 'id_orden':
			encontrado = encontrar_matches(dic_regex[campo], doc_titulo)
		elif campo == 'puesto':
			encontrado = []
			encontrado_denominacion = encontrar_matches(dic_regex[campo]['denominacion'], doc_texto)
			if encontrado_denominacion:
				encontrado += [e.split(':')[1] for e in encontrado_denominacion]	# Quitar denominación y punto final.
			encontrado_cuerpo_escala = encontrar_matches(dic_regex[campo]['cuerpo_escala'], doc_titulo)
			if encontrado_cuerpo_escala:
				encontrado += [e.split(',')[-1][1:-1] for e in encontrado_cuerpo_escala]	# Coger último trozo y quitar espacio y punto final.
			encontrado_especialidad = encontrar_matches(dic_regex[campo]['especialidad'], doc_titulo)
			if encontrado_especialidad:
				encontrado += [e[len('especialidad '):-1] for e in encontrado_especialidad]		# Quitar especialidad y punto final.
			encontrado_identificacion = encontrar_matches(dic_regex[campo]['identificacion'], doc_texto)
			if encontrado_identificacion:
				encontrado += [e[len('IDENTIFICACIÓN DE LA PLAZA: '):] for e in encontrado_identificacion] # Quitar identificación.
		else:
			encontrado = encontrar_matches(dic_regex[campo], doc_texto)
		
		if campo not in ['tipo_convocatoria', 'num_plazas'] and encontrado:
			dic_entidades[campo] = encontrado	# Para evitar devolver listas vacías


	# Post-procesamiento inicial
	if 'web' in dic_entidades.keys():
		webs_invalidas = ['http://','https://','http://www.','https://www.', 'www.']
		dic_entidades['web'] = [web for web in dic_entidades['web'] if web not in webs_invalidas]

	return dic_entidades

def main():

	if len(sys.argv) != 3:
		print('Numero de parametros incorrecto.')
		sys.exit()

	dia = sys.argv[1]
	ruta_info = pathlib.Path(sys.argv[2])
	ruta_texto = pathlib.Path(sys.argv[3])
	ruta_regex = pathlib.Path(sys.argv[4])

	entidades = obtener_campos_reglas(dia, ruta_info, ruta_texto, ruta_regex)

	for e in entidades.items():
		for a in e[1]:
			print(e[0], '--', a)

if __name__ == "__main__":
	main()
