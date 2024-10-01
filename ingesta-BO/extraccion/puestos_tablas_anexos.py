import logging
import re
from pathlib import Path
import pdfplumber
from opentelemetry import trace
from opentelemetry.trace import Tracer


class PuestosTablasAnexos:
    def __init__(self, tracer: Tracer, logger: logging.Logger):
        self.tracer = tracer
        self.logger = logger

    def unica_celda(self, fila, index_puesto):
        """Comprueba si es la única celda no vacía de su fila."""
        with self.tracer.start_as_current_span("Unica Celda") as span:
            es_unica = True
            for i, celda in enumerate(fila):
                if i != index_puesto and self.celda_no_vacia(celda):
                    es_unica = False
                    break
            self.logger.debug(f"Unica celda check: {es_unica}")
            return es_unica

    def celda_no_vacia(self, celda):
        """Comprueba si la celda está vacía."""
        return celda and celda is not None

    def es_un_puesto(self, texto, lista_no_puestos):
        """Para comprobar, mediante la primera palabra, si es un centro directivo o no."""
        return self.celda_no_vacia(texto) and \
               texto.lower().split(' ')[0] not in lista_no_puestos and \
               texto.rstrip(' \t\n.-').lstrip(' \t\n.-') != ''

    def quitar_no_puesto_cola(self, texto, lista_no_puestos):
        """Para quitar centros directivos de la misma celda, que están por detrás para las siguientes celdas."""
        with self.tracer.start_as_current_span("Quitar No Puesto Cola") as span:
            lineas = texto.split('\n')
            puesto = ''
            ha_aparecido_puesto = False
            for linea in lineas:
                if self.es_un_puesto(linea, lista_no_puestos):
                    puesto += ' ' + linea
                    ha_aparecido_puesto = True
                elif ha_aparecido_puesto:
                    break
            self.logger.debug(f"Puesto after removing non-position tail: {puesto}")
            return puesto

    def hay_tabla_puestos(self, tabla, lista_denominaciones):
        """Devuelve True si se ha detectado una tabla con una de las denominaciones en la cabecera."""
        if tabla is None: return False
        if self.indices(tabla, lista_denominaciones) == -1: return False
        return True

    def indices(self, tabla, lista_denominaciones):
        """Devuelve el índice de la denominación encontrada en la cabecera. Sino, -1."""
        with self.tracer.start_as_current_span("Indices") as span:
            i_puesto = -1
            i_cabecera = -1

            for i, e in enumerate(tabla[0]):
                if e is not None:
                    for denominacion in lista_denominaciones: 
                        if denominacion in e.lower():
                            i_puesto = i
                            i_cabecera = 0
                            break
                    if i_cabecera == 0:
                        break

            if i_puesto == -1 and len(tabla) > 1:
                for i, e in enumerate(tabla[1]):
                    if e is not None:
                        for denominacion in lista_denominaciones:
                            if denominacion in e.lower():
                                i_puesto = i
                                i_cabecera = 1
                                break
                        if i_cabecera == 1:
                            break
            span.set_attribute("i_puesto", i_puesto)
            span.set_attribute("i_cabecera", i_cabecera)
            return i_puesto, i_cabecera

    def obtener_puestos_tabla(self, tabla, lista_denominaciones, lista_no_puestos):
        """Devuelve una lista de puestos obtenido de la tabla pasada por argumento."""
        with self.tracer.start_as_current_span("Obtener Puestos Tabla") as span:
            i_puesto, i_cabecera = self.indices(tabla, lista_denominaciones)
            lista_posibles_puestos = []

            if i_puesto > -1:  
                hay_puesto_en_anterior = False
                index_ultimo_puesto = -1
                for i_fila, fila in enumerate(tabla):
                    if i_fila > i_cabecera:
                        texto_celda = fila[i_puesto]
                        col_orden = fila[0]
                        if col_orden is not None:  
                            col_orden = re.sub('[\n ]+', '', col_orden)
                            if self.celda_no_vacia(texto_celda) and col_orden.isnumeric(): 
                                texto_celda = re.sub('[\n ]+', ' ', texto_celda)
                                if hay_puesto_en_anterior and self.unica_celda(fila, i_puesto): 
                                    lista_posibles_puestos[index_ultimo_puesto] = lista_posibles_puestos[index_ultimo_puesto] + ' ' + texto_celda
                                else:  
                                    lista_posibles_puestos.append(texto_celda)
                                    index_ultimo_puesto += 1
                                    hay_puesto_en_anterior = True
                            else:
                                hay_puesto_en_anterior = False
                        else:
                            hay_puesto_en_anterior = False

            lista_puestos = []
            for puesto in lista_posibles_puestos:
                if not puesto.isnumeric() and len(puesto) >= 4:
                    lista_puestos.append(puesto)
            span.set_attribute("puestos_count", len(lista_puestos))
            self.logger.debug(f"Lista de puestos obtenidos: {lista_puestos}")
            return lista_puestos

    def hay_tabla_puestos_documento(self, pdf_path, lista_denominaciones):
        """Comprueba si hay alguna tabla con puestos en el documento."""
        with self.tracer.start_as_current_span("Hay Tabla Puestos Documento") as span:
            try:
                pdf = pdfplumber.open(pdf_path)
                self.logger.debug(f"PDF opened successfully: {pdf_path}")
            except Exception as e:
                msg = f"\nFailed: Read {pdf_path}"
                self.logger.exception(msg)
                span.set_status(trace.Status(trace.StatusCode.ERROR, str(e)))
                raise

            for page in pdf.pages:
                if self.hay_tabla_puestos(page.extract_table(), lista_denominaciones):
                    self.logger.debug(f"Found table with positions on page.")
                    return True
            self.logger.debug(f"No table with positions found in document.")
            return False

    def obtener_puestos_tablas_documento(self, pdf_path, lista_denominaciones, lista_no_puestos):
        """Devuelve una lista de puestos del documento cuyo path se pasa por argumento."""
        with self.tracer.start_as_current_span("Obtener Puestos Tablas Documento") as span:
            try:
                pdf = pdfplumber.open(pdf_path)
                self.logger.debug(f"PDF opened successfully: {pdf_path}")
            except Exception as e:
                msg = f"\nFailed: Read {pdf_path}"
                self.logger.exception(msg)
                span.set_status(trace.Status(trace.StatusCode.ERROR, str(e)))
                raise

            lista_puestos = []
            for page in pdf.pages:
                tabla = page.extract_table()
                if self.hay_tabla_puestos(tabla, lista_denominaciones):

                    lista_puestos += self.obtener_puestos_tabla(tabla, lista_denominaciones, lista_no_puestos)
                    self.logger.debug(f"Puestos encontrados en la página: {len(lista_puestos)}")
            span.set_attribute("puestos_total_count", len(lista_puestos))
            self.logger.debug(f"Total puestos list from document: {lista_puestos}")
            return lista_puestos
