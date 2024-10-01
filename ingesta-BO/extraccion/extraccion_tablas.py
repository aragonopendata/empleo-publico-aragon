import logging
import re
from xml.etree import ElementTree as ET
from pathlib import Path
from puestos_tablas_anexos import PuestosTablasAnexos
from opentelemetry import trace
from opentelemetry.trace import Tracer


class ExtraccionTablas:
    def __init__(self, tracer: Tracer, logger: logging.Logger):
        self.tracer = tracer
        self.logger = logger
        self.puestos_tablas_anexos = PuestosTablasAnexos(self.tracer, self.logger)

    def obtener_lista(self, ruta_aux, campo):
        """Devuelve una lista con las strings del campo indicado del fichero auxiliar indicado."""
        with self.tracer.start_as_current_span("Obtener Lista") as span:
            try:
                with open(ruta_aux, 'rb') as file:
                    tree = ET.parse(file)
                    root = tree.getroot()
                    self.logger.debug(f"File parsed successfully: {ruta_aux}")
            except Exception as e:
                msg = f"\nFailed: Read {ruta_aux}"
                self.logger.exception(msg)
                span.set_status(trace.Status(trace.StatusCode.ERROR, str(e)))
                return []

            item = root.find(campo)
            out = []
            for i in item.findall('./string'):
                out.append(i.text)
            span.set_attribute("lista_count", len(out))
            self.logger.debug(f"List extracted from XML: {out}")
            return out

    def es_lista_correcta(self, lista):
        """Devuelve true si la lista pasada es una lista correcta con puestos."""
        with self.tracer.start_as_current_span("Es Lista Correcta") as span:
            for e in lista:
                count_un_caracter = 0
                for word in re.split(r'\W+', e):
                    if len(word) == 1:
                        count_un_caracter += 1
                if e.count('\n') > 5 or count_un_caracter > 5:
                    self.logger.debug(f"Lista incorrecta: {lista}")
                    return False
            self.logger.debug(f"Lista correcta: {lista}")
            return True

    def elegir_lista(self, lista_n, lista_r):
        """Devuelve una lista de puestos elegida entre las dos pasadas (normal y rotada)."""
        with self.tracer.start_as_current_span("Elegir Lista") as span:
            if lista_n and self.es_lista_correcta(lista_n):
                self.logger.debug("Chosen list: normal")
                return lista_n
            elif lista_r and self.es_lista_correcta(lista_r):
                self.logger.debug("Chosen list: rotated")
                return lista_r
            else:
                self.logger.debug("No correct list found, returning empty list.")
                return []

    def obtener_puestos(self, ruta_pdf, ruta_auxiliar):
        """Devuelve una lista con los puestos obtenidos en tablas en el documento del pdf indicado."""
        with self.tracer.start_as_current_span("Obtener Puestos") as span:
            lista_denominaciones = self.obtener_lista(ruta_auxiliar, 'strings_cabecera_denominaciones')
            lista_no_puestos = self.obtener_lista(ruta_auxiliar, 'strings_no_puestos_tablas')

            puestos_normal = []
            puestos_rotado = []

            if self.puestos_tablas_anexos.hay_tabla_puestos_documento(ruta_pdf, lista_denominaciones):
                puestos_normal = self.puestos_tablas_anexos.obtener_puestos_tablas_documento(ruta_pdf, lista_denominaciones, lista_no_puestos)
                self.logger.debug(f"Puestos normales obtenidos: {puestos_normal}")

            ruta_pdf_rotado = ruta_pdf.parent / 'rotados' / ruta_pdf.name
            if ruta_pdf_rotado.exists() and self.puestos_tablas_anexos.hay_tabla_puestos_documento(ruta_pdf_rotado, lista_denominaciones):
                puestos_rotado = self.puestos_tablas_anexos.obtener_puestos_tablas_documento(ruta_pdf_rotado, lista_denominaciones, lista_no_puestos)
                self.logger.debug(f"Puestos rotados obtenidos: {puestos_rotado}")

            puestos_final = self.elegir_lista(puestos_normal, puestos_rotado)
            span.set_attribute("puestos_count", len(puestos_final))
            self.logger.debug(f"Final puestos list: {puestos_final}")
            return puestos_final
