import logging
import re
from xml.etree import ElementTree as ET
import pathlib
import spacy
from opentelemetry import trace
from opentelemetry.trace import Tracer

class ExtraccionReglas:
    def __init__(self, tracer: Tracer, logger: logging.Logger):
        self.tracer = tracer
        self.logger = logger

    def terminos_a_regex(self, lista_terminos):
        """Concatena los términos de la lista para utilizarlos con un '|' en una regex."""
        with self.tracer.start_as_current_span("Terminos a Regex") as span:
            out = ''
            for termino in lista_terminos:
                out += termino + '|'
            self.logger.debug(f"Regex pattern created: {out[:-1]}")
            return out[:-1]

    def encontrar_matches(self, regex, documento, ignore_case=True):
        with self.tracer.start_as_current_span("Encontrar Matches") as span:
            if ignore_case:
                iter = re.finditer(regex, documento.text, flags=re.IGNORECASE)
            else:
                iter = re.finditer(regex, documento.text)
            
            matches = []
            for match in iter:
                start, end = match.span()
                span_match = documento.text[start:end]
                matches.append(span_match)
            span.set_attribute("matches", matches)
            return matches

    def encontrar_matches_plazo(self, regex, doc):
        with self.tracer.start_as_current_span("Encontrar Matches Plazo") as span:
            encontrados = [e.lower() for e in self.encontrar_matches(regex, doc)]
            if not encontrados: return encontrados
            terminos_eliminatorios = ['obtenido', 'obtenida', 'estas', 'estos', 'esta', 'este', 'de','los','las','en','varios','varias']
            out = []
            for e in encontrados:
                aux = e.split(' ')[0]
                if aux not in terminos_eliminatorios:
                    out.append(e)
            span.set_attribute("filtered_matches_count", len(out))
            self.logger.debug(f"Filtered matches count: {len(out)}")
            return out


    def obtener_root_fichero(self, ruta):
        """Devuelve la raíz del fichero indicado."""
        with self.tracer.start_as_current_span("Obtener Root Fichero") as span:
            try:
                with open(ruta, 'rb') as file:
                    tree = ET.parse(file)
                    root = tree.getroot()
                    self.logger.debug(f"Root parsed from file: {ruta}")
                    return root
            except Exception as e:
                msg = f"\nFailed: Read {ruta}"
                self.logger.exception(msg)
                span.set_status(trace.Status(trace.StatusCode.ERROR, str(e)))
                return None

    def obtener_terminos_tipo(self, root, tipo):
        """Devuelve la lista de términos del tipo tipo aparecidos en el fichero de regex."""
        with self.tracer.start_as_current_span("Obtener Terminos Tipo") as span:
            terminos = []
            for t in root.find('./terminos_tipo/' + tipo).findall('./termino'):
                terminos.append(t.text)
            span.set_attribute("terminos_count", len(terminos))
            self.logger.debug(f"Terms of type {tipo} extracted: {terminos}")
            return terminos
            

    def leer_regla(self, root, regla):
        """Lee la expresión regular indicada del fichero auxiliar indicado."""
        with self.tracer.start_as_current_span("Leer Regla") as span:
            regla_text = root.find('./reglas/apertura/' + regla).text
            span.set_attribute("regla_text", regla_text)
            return regla_text

    def leer_regla_plazo(self, root):
        return root.find('./reglas/apertura/plazo/num_dias').text + \
            root.find('./reglas/apertura/plazo/tipo_dias').text + \
            root.find('./reglas/apertura/plazo/contexto_1').text + \
            root.find('./reglas/apertura/plazo/dia_inicio').text + \
            root.find('./reglas/apertura/plazo/contexto_2').text
    # def leer_regla_plazo(self, root):
    #     """Lee la expresión regular del plazo del fichero auxiliar indicado."""
    #     with self.tracer.start_as_current_span("Leer Regla Plazo") as span:
    #         regla_plazo = ''.join([
    #             root.find('./reglas/apertura/plazo/num_dias').text,
    #             root.find('./reglas/apertura/plazo/tipo_dias').text,
    #             root.find('./reglas/apertura/plazo/contexto_1').text,
    #             root.find('./reglas/apertura/plazo/dia_inicio').text,
    #             root.find('./reglas/apertura/plazo/contexto_2').text
    #         ])
    #         span.set_attribute("regla_plazo", regla_plazo)
    #         self.logger.debug(f"Plazo rule read: {regla_plazo}")
    #         return regla_plazo

    def obtener_campos_reglas(self, dia, ruta_info, ruta_texto, ruta_regex):
        """Obtiene los campos de reglas desde los archivos proporcionados."""
        with self.tracer.start_as_current_span("Obtener Campos Reglas") as span:
            root_info = self.obtener_root_fichero(ruta_info)
            if root_info is None:
                return {}

            titulo = root_info.find('./articulo/titulo').text
            hay_id_orden = root_info.find('./articulo/id_orden') is not None
            hay_f_disp = root_info.find('./articulo/fecha_disposicion') is not None
            rango = root_info.find('./articulo/rango')
            hay_rango = rango is not None and rango.text is not None and rango.text != '-'
            if hay_rango:
                rango = rango.text.replace('ó', 'o').replace('Ó', 'O')

            try:
                with open(ruta_texto, 'r', encoding='utf-8') as file:
                    texto = file.read().split('TEMARIO')[0]
            except Exception as e:
                msg = f"\nFailed: Read {ruta_texto}"
                self.logger.exception(msg)
                span.set_status(trace.Status(trace.StatusCode.ERROR, str(e)))
                return {}

            self.logger.debug(f"Texto cargado y preprocesado desde {ruta_texto}.")
            nlp = spacy.load("es_core_news_md")
            doc_titulo = nlp(titulo)
            doc_texto = nlp(texto)

            root_regex = self.obtener_root_fichero(ruta_regex)
            terminos_ambas = self.obtener_terminos_tipo(root_regex, 'libre_e_interna')
            terminos_libre = self.obtener_terminos_tipo(root_regex, 'libre')
            terminos_interna = self.obtener_terminos_tipo(root_regex, 'interna')

            campos_a_extraer = ['cuerpo', 'escala', 'subescala', 'grupo', 'web', 'email', 'plazo', 'tipo_convocatoria', 'puesto', 'num_plazas']
            if not hay_f_disp:
                campos_a_extraer.append('fecha_disposicion')
            if not hay_id_orden and hay_rango and rango.lower() == 'orden':
                campos_a_extraer.append('id_orden')

            dic_regex = {}
            for campo in campos_a_extraer:
                if campo == 'plazo':
                    dic_regex[campo] = self.leer_regla_plazo(root_regex)
                elif campo == 'tipo_convocatoria':
                    dic_regex[campo] = r"(" + self.terminos_a_regex(terminos_ambas + terminos_libre + terminos_interna) + r")"
                elif campo == 'fecha_disposicion':
                    if hay_rango:
                        if rango.lower() == 'decreto': rango = 'orden'
                        dic_regex[campo] = self.leer_regla(root_regex, campo+'/'+rango.lower())
                    else:
                        dic_regex[campo] = self.leer_regla(root_regex, campo+'/resolucion')
                elif campo == 'puesto':
                    dic_regex[campo] = {}
                    for tipo_puesto in ['denominacion', 'cuerpo_escala', 'especialidad', 'identificacion']:
                        dic_regex[campo][tipo_puesto] = self.leer_regla(root_regex, campo+'/'+tipo_puesto)
                else:
                    dic_regex[campo] = self.leer_regla(root_regex, campo)

            dic_entidades = {}
            for campo in campos_a_extraer:
                if campo in ['tipo_convocatoria', 'num_plazas']:
                    encontrado_ti = self.encontrar_matches_plazo(dic_regex[campo], doc_titulo)
                    encontrado_te = self.encontrar_matches_plazo(dic_regex[campo], doc_texto)
                    if encontrado_ti:
                        dic_entidades[campo+'_titulo'] = encontrado_ti
                    if encontrado_te:
                        dic_entidades[campo+'_texto'] = encontrado_te
                elif campo == 'fecha_disposicion' or campo == 'id_orden':
                    encontrado = self.encontrar_matches(dic_regex[campo], doc_titulo)
                elif campo == 'puesto':
                    encontrado = []
                    encontrado_denominacion = self.encontrar_matches(dic_regex[campo]['denominacion'], doc_texto)
                    if encontrado_denominacion:
                        encontrado += [e.split(':')[1] for e in encontrado_denominacion]
                    encontrado_cuerpo_escala = self.encontrar_matches(dic_regex[campo]['cuerpo_escala'], doc_titulo)
                    if encontrado_cuerpo_escala:
                        encontrado += [e.split(',')[-1][1:-1] for e in encontrado_cuerpo_escala]
                    encontrado_especialidad = self.encontrar_matches(dic_regex[campo]['especialidad'], doc_titulo)
                    if encontrado_especialidad:
                        encontrado += [e[len('especialidad '):-1] for e in encontrado_especialidad]
                    encontrado_identificacion = self.encontrar_matches(dic_regex[campo]['identificacion'], doc_texto)
                    if encontrado_identificacion:
                        encontrado += [e[len('IDENTIFICACIÓN DE LA PLAZA: '):] for e in encontrado_identificacion]
                else:
                    encontrado = self.encontrar_matches(dic_regex[campo], doc_texto)
                
                if campo not in ['tipo_convocatoria', 'num_plazas'] and encontrado:
                    dic_entidades[campo] = encontrado

            if 'web' in dic_entidades.keys():
                webs_invalidas = ['http://', 'https://', 'http://www.', 'https://www.', 'www.']
                dic_entidades['web'] = [web for web in dic_entidades['web'] if web not in webs_invalidas]

            span.set_attribute("extracted_entities_count", len(dic_entidades))
            self.logger.debug(f"Entities extracted: {dic_entidades}")
            return dic_entidades
