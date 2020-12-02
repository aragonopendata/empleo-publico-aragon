# Nombre: extraccion.py
# Autor: Oscar Potrony
# Fecha: 20/11/2020
# Descripción: Evalúa los nuevos artículos cuyo día se pasa como parámetro.
# Invocación:
#	python extraccion.py dia_aaaammdd directorio_base ruta_modelo_NER ruta_regex ruta_auxiliar
# Ejemplo invocación:
#	python extraccion.py 20201001 C:\corpus\ C:\AragonOpenData\aragon-opendata\models\modelo_20201112_50 
#		C:\AragonOpenData\aragon-opendata\tools\ficheros_configuracion\regex.xml
#		C:\AragonOpenData\aragon-opendata\tools\ficheros_configuracion\auxiliar.xml

import sys
import os
import logging
import re

from xml.etree import ElementTree as ET
from pathlib import Path
from datetime import date, timedelta, datetime
from time import strptime
import spacy

import extraccion_reglas
import extraccion_ner
import extraccion_tablas

from workalendar.europe import Spain
from spa2num.converter import to_number
import locale

logger = logging.getLogger('extraccion')
logging.basicConfig()

# create console handler with a higher log level
ch = logging.StreamHandler(sys.stdout)
ch.setLevel(logging.DEBUG)
logger.addHandler(ch)

# Iniciar calendario español
cal = Spain()
locale.setlocale(locale.LC_ALL, 'es_ES')


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

# Devuelve el elemento del metadato indicado de la raíz indicada.
# Para comprobar si existe, mirar si no es None. Para el texto, .text
def obtener_metadato(root, metadato):
	return root.find('./articulo/' + metadato)

# Devuelve los matches del documento con la regex pasada.
def encontrar_matches(regex, documento, ignore_case=True):
	if ignore_case:
		iter = re.finditer(regex, documento.text, flags=re.IGNORECASE)
	else:
		iter = re.finditer(regex, documento.text)
	
	matches = []
	for match in iter:
		start, end = match.span()
		span = documento.char_span(start, end)
		
		if span is not None:
			matches.append(span.text)
	return matches

# Devuelve una lista de los grupos encontrados en la lista sin limpiar pasada.
def limpiar_grupos(lista_grupos):
	regex_posibles_grupos = '(A1|A2|A|B|C1|C2|C|E|GP1|GP2|GP3|GP4|GP5)'
	out = []
	nlp = spacy.load("es_core_news_md")
	for texto in lista_grupos:
		doc = nlp(texto)
		encontrado = encontrar_matches(regex_posibles_grupos, doc, False)
		if not encontrado:
			break
		elif len(encontrado) == 1:
			out.append(encontrado[0])
		else:
			for e in encontrado: out.append(e)
		
	return out

# Devuelve la concatenación de todos los grupos únicos encontrados.
def juntar_grupos(lista_1, lista_2):
	if lista_1 or lista_2:
		return '/'.join(sorted(list(set(lista_1 + lista_2))))
	else:
		return '-'

# Devuelve la lista de términos del tipo tipo aparecidos en el fichero de regex
def obtener_terminos_tipo(root, tipo):
	terminos = []
	for t in root.find('./terminos_tipo/' + tipo).findall('./termino'):
		terminos.append(t.text)
	return terminos

def obtener_tipo(root_regex, tit_reg, tex_reg, tex_ner):
	terminos_ambas = obtener_terminos_tipo(root_regex, 'libre_e_interna')
	terminos_libre = obtener_terminos_tipo(root_regex, 'libre')
	terminos_interna = obtener_terminos_tipo(root_regex, 'interna')

	# Si se encuentra un término de ambas o términos de libre e interna en el título
	# mediante las reglas, ambas. Si no, el que se encuentre.
	tr_libre_reglas, tr_interna_reglas = 0, 0
	for tr in tit_reg:
		if tr in terminos_ambas: return 'Libre+Interna'
		elif tr in terminos_libre: tr_libre_reglas += 1
		elif tr in terminos_interna: tr_interna_reglas += 1
	if tr_libre_reglas and tr_interna_reglas: return 'Libre+Interna'
	elif tr_libre_reglas: return 'Libre'
	elif tr_interna_reglas: return 'Interna'

	# Si no, se mira el texto con reglas. Si hay un término de ambas o términos de 
	# libre e interna, ambas.
	tr_libre_reglas, tr_interna_reglas = 0, 0
	for tr in tex_reg:
		if tr in terminos_ambas: return 'Libre+Interna'
		elif tr in terminos_libre: tr_libre_reglas += 1
		elif tr in terminos_interna: tr_interna_reglas += 1
	if tr_libre_reglas and tr_interna_reglas: return 'Libre+Interna'

	# Si no, se mira el texto con ner. Si hay un término de ambas o términos de 
	# libre e interna, ambas.
	tr_libre_ner, tr_interna_ner = 0, 0
	for tr in tex_reg:
		if tr in terminos_ambas: return 'Libre+Interna'
		elif tr in terminos_libre: tr_libre_ner += 1
		elif tr in terminos_interna: tr_interna_ner += 1
	if tr_libre_ner and tr_interna_ner: return 'Libre+Interna'

	# Si no, lo que se encuentre en el texto con reglas. Si no, lo que se encuentre con NER.
	if tr_libre_reglas: return 'Libre'
	elif tr_interna_reglas: return 'Interna'
	elif tr_libre_ner: return 'Libre'
	elif tr_interna_ner: return 'Libre'

	# Por defecto libre. Útil y correcto especialmente para los boletines provinciales.
	return 'Libre'

# Devuelve un dato de contacto de los pasados (websites e emails).
def obtener_datos_contacto(web_reg, em_reg, web_ner, em_ner):
	# Preferencia a las reglas por ser más específicas.
	# Poca preferencia a los emails de ner porque se han etiquetado pocos casos.
	if web_reg: return web_reg[0]
	elif em_reg: return em_reg[0]
	elif web_ner: return web_ner[0]
	elif em_ner: return em_ner[0]
	else: return '-'

# Devuelve el primer elemento de la lista primera.
# Si no lo hay, el primero de la segunda. Sino, '-'.
def primero_por_preferencia(lista1, lista2):
	if lista1: return lista1[0]
	elif lista2: return lista2[0]
	else: return '-'

# Devuelve las fechas de inicio y fin de presentación de solicitudes,
# detectadas a partir del plazo y la fecha de publicación.
def obtener_fechas_presentacion(root_regex, plazo, fpub):
	if plazo == '-': return '-', '-'
	regex_tipo_dias = root_regex.find('./reglas/apertura/plazo/tipo_dias').text
	regex_contexto_1 = root_regex.find('./reglas/apertura/plazo/contexto_1').text
	regex_dia_inicio = root_regex.find('./reglas/apertura/plazo/dia_inicio').text

	# Cifra de días.
	match = re.search(regex_tipo_dias+regex_contexto_1+regex_dia_inicio, plazo)
	dias = to_number(plazo[:match.span()[0]].split('(')[0])
	plazo = plazo[match.span()[0]:]

	# Saber si habla de días o meses, y si son hábiles o naturales.
	match = re.match(regex_tipo_dias, plazo)
	split = plazo[match.span()[0]:match.span()[1]].split(' ')
	if 'mes' in split[1]:
		dias *= 30
	# Se han encontrado 'naturales', 'hábiles' o nada. Si no hay nada, se entienden como naturales.
	es_habil = len(split) > 2 and ('hábil' in split[2] or 'laborable' in split[2])

	# Averiguar si se cuenta a partir del día de publicación o el siguiente.
	plazo = plazo[match.span()[1]:]
	match = re.match(regex_contexto_1, plazo)
	plazo = plazo[match.span()[1]:]
	match = re.match(regex_dia_inicio, plazo)
	es_dia_siguiente = 'siguiente' in plazo[:match.span()[1]]

	# Calcular fecha de inicio
	inicio = date(int(fpub[6:]), int(fpub[3:5]), int(fpub[:2]))			# fpub viene con formato 'dd/mm/yyyy'
	if es_dia_siguiente:
		inicio += timedelta(days=1)
	fecha_inicio = inicio.strftime('%d/%m/%Y')

	# Calcular fecha de fin
	if es_habil:
		fecha_fin = cal.add_working_days(inicio, dias)
	else:
		fecha_fin = inicio + timedelta(days=dias)
	fecha_fin = fecha_fin.strftime('%d/%m/%Y')

	return fecha_inicio, fecha_fin

# Devuelve el texto pasado, limpiado y convertido a número
def limpiar_texto_plazas(texto):
	texto = texto.lower()

	# Tratar los números de plazas compuestos
	if ' y ' in texto:
		split_y = texto.split(' ')
		for i, l in enumerate(split_y):
			if l == 'y':
				i_y = i
		texto = split_y[i_y-1] + ' y ' + split_y[i_y+1]
		try:
			texto = to_number(texto)
			return texto
		except:
			texto = split_y[i_y+1]

	terminos_a_eliminar = ['puestos', 'vacantes', 'plazas', 'puesto', 'vacante', 'plaza', 'número de', ':', 'totales']
	terminos_una_plaza = ['una', 'un', 'del', 'el', 'la']
	for t in terminos_a_eliminar:
		texto = texto.replace(t, '')
	texto = texto.rstrip().lstrip()
	if texto in terminos_una_plaza: texto = '1'
	try:
		texto = to_number(texto)
	except:
		texto = '-'
	
	return texto


# Devuelve el número de plazas totales, que ha de ser mayor o igual que num_puestos.
def obtener_num_plazas(tit_reg, tex_reg, tex_ner, num_puestos):
	# Si se ha encontrado en el título, elegir la primera encontrada ahí
	if tit_reg:
		for p in tit_reg:
			p = limpiar_texto_plazas(p)
			if p != '-': return p

	if len(tex_reg) + len(tex_ner) == 0: return '-'

	# Limpiar todas las ocurrencias encontradas
	lista = []
	for i, t in enumerate(tex_reg+tex_ner):
		t = limpiar_texto_plazas(t)
		if t != '-':
			lista.append(t)

	# Eliminar duplicados y ordenar de mayor a menor.
	lista = sorted(list(set(lista)), reverse=True)

	# Devolver el mayor número encontrado, si es mayor o igual que el número de puestos.
	if lista: return lista[0] if lista[0] >= num_puestos else num_puestos
	else: return '-'

# Devuelve una lista con stopwords en español, leída del fichero ../ficheros_configuracion/stopwords.txt
def leer_stopwords():
	out = []
	with open(Path('../ficheros_configuracion/stopwords.txt'), encoding='utf-8') as file:
		for line in file:
			out.append(line.rstrip('\n'))
	return out

# Quita escalas incorrectas que ha podido coger el ner
def quitar_escalas_incorrectas(lista):
	cadenas = ['escala a que pertenece', 'escala a la que pertenece', 'escala grupo', 'escala o clase de especialidad',
			   'escala situación administrativa', 'escala por promoción', 'escala de procedencia', 'escala subgrupo',
			   'escala grup/subg']
	cortas_correctas = ['de', 'y']
	out = []
	for e in lista:
		meter = True
		for cadena in cadenas:
			if cadena in e.lower(): meter = False
		for word in e.lower().split(' '):
			if len(word) < 4 and word not in cortas_correctas: meter = False
		if meter: out.append(e)
	return out

# Quita cuerpos incorrectos que ha podido coger el ner
def quitar_cuerpos_incorrectos(lista):
	cortas_correctas = ['de', 'y']
	out = []
	for e in lista:
		meter = True
		for word in e.lower().split(' '):
			if len(word) < 4 and word not in cortas_correctas: meter = False
		if meter: out.append(e)
	return out

# Devuelve texto, sin stopwords al principio ni al final del mismo.
def evitar_extremo_stopword(texto, stopwords):
	texto = texto.rstrip(' \t\n.:,(')
	while texto != '':
		split = texto.split(' ')
		if split[-1] not in stopwords:
			break
		else:
			texto = ' '.join(split[0:-1])
			texto = texto.rstrip(' \t\n.:,(')

	texto = texto.lstrip(' \t\n.:,)')
	while texto != '':
		split = texto.split(' ')
		if split[-1] not in stopwords:
			break
		else:
			texto = ' '.join(split[0:-1])
			texto = texto.lstrip(' \t\n.:,)')

	return texto

# Quitar de lista los elementos que empiezan por una stopword
def quitar_puestos_con_inicio_stopword(lista, stopwords):
	out = []
	for e in lista:
		e = e.lstrip(' \t\n.-')
		if e.split(' ')[0] not in stopwords:
			out.append(e)
	return out

# Devuelve el puesto del ner tras quitarle ciertas substrings.
def limpiar_puesto_ner(texto, stopwords):
	terminos_a_eliminar = ['especialidad']
	terminos_eliminatorios = ['denominacion', 'denominación']

	# Si un puesto de NER contiene denominación, se desecha
	for t in terminos_eliminatorios:
		if t in texto:
			return None
	
	# Quitar substrings a eliminar del texto
	for t in terminos_a_eliminar:
		texto = texto.replace(t, '')

	# Quitar stopwords de extremos
	texto = evitar_extremo_stopword(texto, stopwords)
	if texto == '': return None
	
	return texto

# Devuelve los puestos del ner pasados como lista, tras quitarles ciertas substrings.
def limpiar_puestos_ner(lista, stopwords):
	out = []
	for e in lista:
		e = limpiar_puesto_ner(e, stopwords)
		if e is not None:
			out.append(e)

	return out

# Devuelve una lista con los puestos detectados.
# (Los puestos de reglas vienen más o menos limpios, mientras que los del ner no)
def obtener_puestos(p_reg, p_ner, p_tablas, stopwords):
	if p_tablas:
		return [p.lower().replace('código de puesto','').rstrip(' \t\n.:,–-(0123456789)').lstrip(' \t\n.:,)-').capitalize() for p in p_tablas]
	if p_reg:
		return [p_reg[0].lower().rstrip(' \t\n.:,(').lstrip(' \t\n.:,)-').capitalize()]

	p_ner = limpiar_puestos_ner([e.lower() for e in p_ner], stopwords)
	if p_ner:
		return [p_ner[0].capitalize()]
	else:
		return '-'

# Devuelve la fecha de disposición limpia (la primera de la lista, que es no vacía).
def obtener_fecha_disposicion(lista, fpub, root_aux):
	if not lista: return fpub
	split = [e.rstrip().lstrip() for e in lista[0].split('de') if e]
	dia = split[0].zfill(2)
	mes = root_aux.find('./correspondencias_meses/' + split[1].lower()).text
	if len(split) > 2:
		anyo = split[2].zfill(4)
	else:
		anyo = int(fpub.split('/')[-1])
		fpub_str = strptime(fpub, "%d/%m/%Y")
		d_p = datetime(fpub_str[0],fpub_str[1],fpub_str[2])

		# No se sabe el año, por lo que se pondrá el pasado más cercano a la fecha de publicación
		anyo_descubierto = False
		while not anyo_descubierto and anyo > 1900:
			fdisp_str = strptime(dia + '/' + split[1].lower() + '/' + str(anyo), "%d/%B/%Y")	# Se pasa el nombre del mes en español por el locale indicado.
			d_d = datetime(fdisp_str[0],fdisp_str[1],fdisp_str[2])
			
			# Si con este año la fecha de disposición es anterior a la fecha de publicación -> Se elige este año
			if (d_d-d_p).days < 0:
				anyo_descubierto = True
				anyo = str(anyo)
			else:
				anyo -= 1

	return dia + '/' + mes + '/' + anyo

# Devuelve lista, sin los elementos que no contienen el texto texto.
def limpiar_por_texto(lista, texto):
	if not lista: return lista
	out = []
	for e in lista:
		e = e.lower()
		if texto in e: out.append(e.lstrip(' \t\n.,:').rstrip(' \t\n.,:').capitalize())
	return out

# Devuelve el subelemento pasado tras incorporarle el campo indicado con el texto indicado.
def escribir_en_info(SE, campo, texto):
	campo_ET = ET.SubElement(SE, campo)
	campo_ET.text = texto
	return SE

# Devuelve el subelemento pasado tras incorporarle los puestos indicados.
def escribir_puestos_en_info(SE, puestos):
	if puestos == '-': return escribir_en_info(SE, 'puestos', puestos)
	
	# Etiqueta exterior 'puestos'
	puestos_ET = ET.SubElement(SE, 'puestos')

	# Subetiquetas 'puesto' con cada puesto encontrado
	for puesto in puestos:
		puesto_ET = ET.SubElement(puestos_ET, 'puesto')
		puesto_ET.text = puesto

	return SE

# Añade los puestos leídos de tablas en el fichero pdf pasado al fichero de info
def evaluar_tablas_cierre(ruta_info, ruta_pdf, ruta_auxiliar):
	root_info = obtener_root_fichero(ruta_info)
	stopwords = leer_stopwords()

	# Obtener puestos de las tablas
	puestos_tablas = quitar_puestos_con_inicio_stopword(extraccion_tablas.obtener_puestos(ruta_pdf, ruta_auxiliar), stopwords)

	# Incorporar los campos al árbol de info
	articulo = root_info.find('articulo')
	articulo = escribir_puestos_en_info(articulo, puestos_tablas)

	# Escribir fichero de info
	with open(ruta_info,'wb') as file:
		tree_info = ET.ElementTree(root_info)
		tree_info.write(file)


# Modifica el fichero info pasado con todos los campos encontrados agregados.
def evaluar_articulo(dia, ruta_info, ruta_texto, ruta_pdf, ruta_modelo_NER, ruta_regex, ruta_auxiliar):
	print('\nArticulo', ruta_texto)
	root_info = obtener_root_fichero(ruta_info)
	root_regex = obtener_root_fichero(ruta_regex)
	root_auxiliar = obtener_root_fichero(ruta_auxiliar)
	fecha_publicacion = obtener_metadato(root_info, 'fecha_publicacion').text
	stopwords = leer_stopwords()
	ents_reglas = extraccion_reglas.obtener_campos_reglas(dia, ruta_info, ruta_texto, ruta_regex)
	keys_reglas = ents_reglas.keys()
	ents_ner = extraccion_ner.obtener_campos_ner(ruta_texto, ruta_modelo_NER)
	keys_ner = ents_ner.keys()
	puestos_tablas = quitar_puestos_con_inicio_stopword(extraccion_tablas.obtener_puestos(ruta_pdf, ruta_auxiliar), stopwords)
	print('Reglas', ents_reglas)
	print('NER', ents_ner)
	print('Tablas', puestos_tablas)

	## Campos que aparecen siempre en metadatos:
	#	fuente_datos, fecha_publicacion, enlace_convocatoria, organo_convocante, titulo, uri_eli*, rango*
	#	* Pueden no aparecer, lo que indica que no existe.
	
	## enlace_cierre no se requiere en la apertura, y estado no hace falta detectarlo

	## Campos que aparecen en ocasiones en metadatos:

	# ID de la orden (Si no está como metadato y sí como regla, cogerlo).
	escribir_id_orden = obtener_metadato(root_info, 'id_orden') is None and 'id_orden' in keys_reglas and ents_reglas['id_orden']
	if escribir_id_orden: id_orden = ents_reglas['id_orden'][0].split(' ')[1]	# Quitar la palabra Orden

	# Fecha de disposición (Si no está como metadato, coger la primera por reglas. Si no la hay, poner la fecha de publicación).
	escribir_fdisp = obtener_metadato(root_info, 'fecha_disposicion') is None
	if escribir_fdisp:
		fecha_disposicion = obtener_fecha_disposicion(
					ents_reglas['fecha_disposicion'] if 'fecha_disposicion' in keys_reglas else [],
					fecha_publicacion,
					root_auxiliar)


	## Los demás campos:

	# Escala
	escala = evitar_extremo_stopword(primero_por_preferencia(
					quitar_escalas_incorrectas(limpiar_por_texto(ents_ner['escala'] if 'escala' in keys_ner else [], 'escala')),
					quitar_escalas_incorrectas(ents_reglas['escala']) if 'escala' in keys_reglas else []
					), stopwords).capitalize()

	# Subescala
	subescala = evitar_extremo_stopword(primero_por_preferencia(
					limpiar_por_texto(ents_ner['subescala'] if 'subescala' in keys_ner else [], 'subescala'),
					ents_reglas['subescala'] if 'subescala' in keys_reglas else []
					), stopwords).capitalize()

	# Cuerpo
	cuerpo = evitar_extremo_stopword(primero_por_preferencia(
					quitar_cuerpos_incorrectos(limpiar_por_texto(ents_ner['cuerpo'] if 'cuerpo' in keys_ner else [], 'cuerpo')),
					quitar_cuerpos_incorrectos(ents_reglas['cuerpo']) if 'cuerpo' in keys_reglas else []
					), stopwords).capitalize()

	# Grupo
	grupo = juntar_grupos(
					limpiar_grupos(ents_reglas['grupo']) if 'grupo' in keys_reglas else [],
					limpiar_grupos(ents_ner['grupo']) if 'grupo' in keys_ner else [],
					)

	# Tipo_convocatoria
	tipo_convocatoria = obtener_tipo(
					root_regex,
					ents_reglas['tipo_convocatoria_titulo'] if 'tipo_convocatoria_titulo' in keys_reglas else [],
					ents_reglas['tipo_convocatoria_texto'] if 'tipo_convocatoria_texto' in keys_reglas else [],
					ents_ner['tipo_convocatoria'] if 'tipo_convocatoria' in keys_ner else []
					)

	# Datos de contacto
	datos_contacto = obtener_datos_contacto(
					ents_reglas['web'] if 'web' in keys_reglas else [],
					ents_reglas['email'] if 'email' in keys_reglas else [],
					ents_ner['web'] if 'web' in keys_ner else [],
					ents_ner['email'] if 'email' in keys_ner else []
					)

	# Plazo
	plazo = primero_por_preferencia(
					ents_reglas['plazo'] if 'plazo' in keys_reglas else [],
					ents_ner['plazo'] if 'plazo' in keys_ner else []
					)

	# Fechas de inicio y fin de presentación de solicitudes
	fecha_inicio_presentacion, fecha_fin_presentacion = obtener_fechas_presentacion(root_regex, plazo, fecha_publicacion)

	# Puestos
	puestos = obtener_puestos(
					ents_reglas['puesto'] if 'puesto' in keys_reglas else [],
					ents_ner['puesto'] if 'puesto' in keys_ner else [],
					puestos_tablas,
					stopwords
					)

	# Número de plazas totales.
	num_plazas = obtener_num_plazas(
					ents_reglas['num_plazas_titulo'] if 'num_plazas_titulo' in keys_reglas else [],
					ents_reglas['num_plazas_texto'] if 'num_plazas_texto' in keys_reglas else [],
					ents_ner['num_plazas'] if 'num_plazas' in keys_ner else [],
					len(puestos)
					)

	# Incorporar los campos al árbol de info
	articulo = root_info.find('articulo')
	if escribir_id_orden: articulo = escribir_en_info(articulo, 'id_orden', id_orden)
	if escribir_fdisp: articulo = escribir_en_info(articulo, 'fecha_disposicion', fecha_disposicion)
	articulo = escribir_en_info(articulo, 'escala', escala)
	articulo = escribir_en_info(articulo, 'subescala', subescala)
	articulo = escribir_en_info(articulo, 'cuerpo', cuerpo)
	articulo = escribir_en_info(articulo, 'grupo', grupo)
	articulo = escribir_en_info(articulo, 'tipo_convocatoria', tipo_convocatoria)
	articulo = escribir_en_info(articulo, 'datos_contacto', datos_contacto)
	articulo = escribir_en_info(articulo, 'plazo', plazo)
	articulo = escribir_en_info(articulo, 'fecha_inicio_presentacion', fecha_inicio_presentacion)
	articulo = escribir_en_info(articulo, 'fecha_fin_presentacion', fecha_fin_presentacion)
	articulo = escribir_en_info(articulo, 'num_plazas', str(num_plazas))
	articulo = escribir_puestos_en_info(articulo, puestos)

	# Escribir fichero de info
	with open(ruta_info,'wb') as file:
		tree_info = ET.ElementTree(root_info)
		tree_info.write(file)

# Extrae los campos de todos los ficheros de directorio_base / dia, guardando
# las extracciones en sus respectivos ficheros de info.
def evaluar_todos(dia, directorio_base, ruta_modelo_NER, ruta_regex, ruta_auxiliar):

	for filename in os.listdir(directorio_base / dia / 'apertura' / 'txt'):
		if '_legible' not in filename:
			ruta_texto = directorio_base / dia / 'apertura' / 'txt' / filename
			ruta_pdf = directorio_base / dia / 'apertura' / 'pdf' / (filename.replace('.txt', '.pdf'))
			ruta_info = directorio_base / dia / 'apertura' / 'info' / (filename.replace('.txt', '.xml'))

			evaluar_articulo(dia, ruta_info, ruta_texto, ruta_pdf, ruta_modelo_NER, ruta_regex, ruta_auxiliar)

	for filename in os.listdir(directorio_base / dia / 'cierre' / 'info'):
		ruta_info = directorio_base / dia / 'cierre' / 'info' / filename
		ruta_pdf = directorio_base / dia / 'cierre' / 'pdf' / (filename.replace('.xml', '.pdf'))
		evaluar_tablas_cierre(ruta_info, ruta_pdf, ruta_auxiliar)

# Evalúa únicamente el artículo del caso indicado, contando previamente
# con una estructura definida.
def evaluar_pruebas_aceptacion(caso):
	ruta_modelo_NER = Path(r'C:\AragonOpenData\aragon-opendata\models\modelo_20201112_50')
	ruta_regex = Path(r'C:\AragonOpenData\aragon-opendata\tools\ficheros_configuracion\regex.xml')
	ruta_auxiliar = Path(r'C:\AragonOpenData\aragon-opendata\tools\ficheros_configuracion\auxiliar.xml')
	ruta_casos = Path(r'C:\Users\opotrony\Desktop\Artículos de casos de prueba')

	cwd = ruta_casos / ('Caso_' + caso)
	for file in os.listdir(cwd):
		if '.pdf' in file:
			ruta_pdf = cwd / file
		elif '.xml' in file and 'copia' not in file:
			ruta_info = cwd / file
		elif '.txt' in file:
			ruta_txt = cwd / file

	evaluar_articulo(ruta_pdf.name.split('_')[-2], ruta_info, ruta_txt, ruta_pdf, ruta_modelo_NER, ruta_regex, ruta_auxiliar)


def main():

	if len(sys.argv) == 2:
		evaluar_pruebas_aceptacion(sys.argv[1])
	elif len(sys.argv) != 6:
		print('Numero de parametros incorrecto.')
		sys.exit()
	else:
		dia = sys.argv[1]
		directorio_base = Path(sys.argv[2])
		ruta_modelo_NER = Path(sys.argv[3])
		ruta_regex = Path(sys.argv[4])
		ruta_auxiliar = Path(sys.argv[5])
		evaluar_todos(dia, directorio_base, ruta_modelo_NER, ruta_regex, ruta_auxiliar)

if __name__ == "__main__":
	main()
