import logging
import re
from xml.etree import ElementTree as ET
import pathlib
import spacy
from opentelemetry import trace
from opentelemetry.trace import Tracer

class ExtraccionNER:
    def __init__(self, tracer: Tracer, logger: logging.Logger):
        self.tracer = tracer
        self.logger = logger

    def evaluate_model(self, test_text: str, model_path: str):
        """Evalúa el modelo NER y devuelve las entidades obtenidas y el documento del texto."""
        with self.tracer.start_as_current_span("Evaluate Model") as span:
            self.logger.debug(f"Evaluando modelo en {model_path} con texto de prueba proporcionado.")
            
            # Cargando el modelo Spacy
            nlp = spacy.load(model_path)
            doc = nlp(test_text)

            entidades = []
            for ent in doc.ents:
                entidad = (ent.label_, ent.text)
                entidades.append(entidad)
            span.set_attribute("model_path", model_path)
            span.set_attribute("entities_count", len(entidades))

            return entidades, doc

    def segmentar(self, texto, max_length, corte):
        """Segmenta el texto en partes basadas en la longitud máxima y el carácter de corte."""
        with self.tracer.start_as_current_span("Segment Text") as span:
            self.logger.debug(f"Segmentando texto con longitud máxima {max_length} usando el carácter de corte '{corte}'.")

            textos = []
            lineas = texto.split(corte)

            if corte != ' ':
                lineas_aux = lineas
                lineas = []
                for i_l, linea in enumerate(lineas_aux):
                    if len(linea) > max_length:
                        for s in self.segmentar(linea, max_length, ' '):
                            lineas.append(s)
                    else:
                        lineas.append(linea)
                del(lineas_aux)

            iter_l = iter(lineas)
            actual = next(iter_l)
            for siguiente in iter_l:
                if len(actual) + len(corte) + len(siguiente) > max_length:
                    textos.append(actual)
                    actual = siguiente
                else:
                    actual += corte + siguiente

            textos.append(actual)
            span.set_attribute("segments_count", len(textos))
            return textos

    def obtener_campos_ner(self, ruta_texto, ruta_modelo):
        """Obtiene los campos NER desde el texto dado y el modelo."""
        with self.tracer.start_as_current_span("Obtain NER Fields") as span:
            try:
                with open(ruta_texto, 'r', encoding='utf-8') as file:
                    texto_completo = file.read()
            except Exception as e:
                msg = f"\nFailed: Read {ruta_texto}"
                self.logger.exception(msg)
                span.set_status(trace.Status(trace.StatusCode.ERROR, str(e)))
                return

            self.logger.debug(f"Texto cargado desde {ruta_texto}. Longitud del texto: {len(texto_completo)} caracteres.")

            texto_completo = texto_completo.split('TEMARIO')[0]
            span.set_attribute("text_length", len(texto_completo))

            max_length = 3100
            corte = '. '
            segmentos = self.segmentar(texto_completo, max_length, corte)

            entidades_encontradas = []
            for segmento in segmentos:
                ents = self.evaluate_model(segmento, ruta_modelo)[0]
                if ents:
                    for ent in ents:
                        entidades_encontradas.append(ent)

            # Post-procesamiento de subescalas
            for i, (et, an) in enumerate(entidades_encontradas):
                if et == 'escala' and 'subescala' in an.lower():
                    entidades_encontradas[i] = ('subescala', an)

            # Crear diccionario de entidades
            dic_entidades = {}
            for et, an in entidades_encontradas:
                if et not in dic_entidades.keys():
                    dic_entidades[et] = []
                
                dic_entidades[et].append(an)

            span.set_attribute("entities_extracted", len(dic_entidades))
            return dic_entidades