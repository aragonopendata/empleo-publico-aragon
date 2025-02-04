import re
import logging
from pathlib import Path
from xml.etree import ElementTree as ET

import pdfplumber
import textwrap3

class PDFError(Exception):
    pass

class OutputTextError(Exception):
    pass

class PDFToTextConverter:
    MAX_CHARS_PER_LINE = 500
    TIPO_BOLETINES = ['BOE', 'BOA', 'BOPH', 'BOPZ', 'BOPT']

    def __init__(self, tracer, logger):
        self.tracer = tracer
        self.logger = logger

    # Concatena las partes de las palabras que quedan cortadas por fin de línea física (indicado con -).
    # Parámetro extra si se quieren subir a la línea superior también los puntos y los dos puntos.
    def quitar_guion_fin_renglon(self, texto, extra=True):
        with self.tracer.start_as_current_span('quitar_guion_fin_renglon') as span:
            span.set_attribute('texto', texto)
            if extra:
                return re.sub(r'([a-zA-Z0-9áéíóúÁÉÍÓÚñÑ]+)-\n([a-zA-Z0-9áéíóúÁÉÍÓÚñÑ,;.:)]+)', r'\1\2\n',texto)
            else:
                return re.sub(r'([a-zA-Z0-9áéíóúÁÉÍÓÚñÑ]+)-\n([a-zA-Z0-9áéíóúÁÉÍÓÚñÑ,;)]+)', r'\1\2\n',texto)


    # Quita los espacios en blanco tras un punto (o dos puntos) y aparte.
    def quitar_blankspaces_finales(self, texto):
        return re.sub(r'([.:])[ ]+\n',r'\1\n', texto)

    # Juntar por párrafos, a excepción de títulos (por si terminan en espacio).
    def juntar_por_parrafos(self, texto):
        return re.sub(r'([^A-Z]) \n|([^A-Z])\n ', r'\1\2 ', texto)

    # Juntar por párrafos, identificándolos en función de su punto y aparte (o dos puntos).
    def juntar_por_parrafos_punto(self, texto):
        texto = re.sub(r'([^.:])\n', r'\1 ', texto)
        return re.sub('[ ]+',' ', texto)

    def diccionario_meses(self):
        ruta_fcs = Path(__file__).absolute().parent.parent / 'ficheros_configuracion'
        ruta_fichero_aux = ruta_fcs / 'auxiliar.xml'
        try:
            with open(ruta_fichero_aux, 'rb') as file:
                tree_aux = ET.parse(file)
                root_aux = tree_aux.getroot()
                self.logger.info(f"Month dictionary loaded successfully: {ruta_fichero_aux}")
                out = {}
                iter = root_aux.find('./correspondencias_meses').iter()
                next(iter)
                for t in iter:
                    out[t.tag] = t.text

                return out
        except Exception as e:
            msg = f"Failed to open month dictionary file: {ruta_fichero_aux}"
            self.logger.exception(msg)
            raise

    def recuperar_fichero_configuracion(self, tipo_boletin):
        ruta_fcs = Path(__file__).absolute().parent.parent / 'ficheros_configuracion'
        ruta_fichero_conf = ruta_fcs / (tipo_boletin + '_conf.xml')
        if not ruta_fichero_conf.exists():
            self.logger.info(f"Configuration file not found: {ruta_fichero_conf}")
            return None
        try:
            with open(ruta_fichero_conf, 'rb') as file:
                tree = ET.parse(file)
                root = tree.getroot()
                self.logger.info(f"Configuration file loaded successfully: {ruta_fichero_conf}")
                return root
        except Exception as e:
            msg = f"Failed to open configuration file: {ruta_fichero_conf}"
            self.logger.exception(msg)
            raise

    def from_pdf_to_text(self, input_filepath, output_filepath, tipo_boletin, legible=False):
        with self.tracer.start_as_current_span('from_pdf_to_text') as span:
            span.set_attribute('input_filepath', str(input_filepath))
            span.set_attribute('output_filepath', str(output_filepath))
            span.set_attribute('tipo_boletin', tipo_boletin)
            span.set_attribute('legible', legible)

            try:
                pdf = pdfplumber.open(input_filepath)
                span.set_attribute('pdf_opened', True)
            except Exception as e:
                msg = f"Failed to read PDF file: {input_filepath}"
                self.logger.exception(msg)
                span.set_attribute('pdf_opened', False)
                span.record_exception(e)
                raise PDFError(msg) from e

            root_fc = self.recuperar_fichero_configuracion(tipo_boletin)
            if root_fc is not None:
                bounding_box = (
                    int(root_fc.find('./puntos_corte/x0').text),
                    int(root_fc.find('./puntos_corte/top').text),
                    int(root_fc.find('./puntos_corte/x1').text),
                    int(root_fc.find('./puntos_corte/bottom').text)
                )
                bounding_box_fecha = (
                    int(root_fc.find('./puntos_corte/x0_fecha').text),
                    int(root_fc.find('./puntos_corte/top_fecha').text),
                    int(root_fc.find('./puntos_corte/x1_fecha').text),
                    int(root_fc.find('./puntos_corte/bottom_fecha').text)
                )
            else:
                bounding_box = (50, 100, 500, 800)
                bounding_box_fecha = (150, 75, 400, 90)
                span.set_attribute('bounding_box_default', True)

            texto = ''
            for page in pdf.pages:
                try:
                    cropped_page = page.crop(bounding_box)
                    extracted_text = cropped_page.extract_text()
                    if extracted_text is not None:
                        texto += extracted_text + '\n'
                    span.set_attribute('text_extracted', True)
                except Exception as e:
                    msg = f"Warning: Failed to read page from PDF: {input_filepath}"
                    self.logger.exception(msg)
                    span.set_attribute('text_extracted', False)

            if tipo_boletin == 'BOPT':
                texto = self.quitar_blankspaces_finales(texto)

            if tipo_boletin in ['BOE', 'BOA', 'BOPZ', 'BOPT']:
                texto = self.quitar_guion_fin_renglon(texto)
                texto = self.juntar_por_parrafos(texto)
            elif tipo_boletin == 'BOPH':
                split = texto.split('\n')
                num_articulo = False
                indice_inicio_texto = 0
                for i, e in enumerate(split):
                    if len(e.split(' ')) > 7 or not e.isupper():
                        if num_articulo:
                            indice_inicio_texto = i
                            break
                        else:
                            num_articulo = True
                string_inicial = '\n'.join(split[:i])
                string_texto = '\n'.join(split[i:])
                string_texto = self.juntar_por_parrafos_punto(self.quitar_guion_fin_renglon(string_texto, False))
                texto = string_inicial + '\n' + string_texto
            else:
                texto = self.quitar_guion_fin_renglon(texto)
                texto = self.juntar_por_parrafos(texto)

            page = pdf.pages[0]
            fecha = page.crop(bounding_box_fecha).extract_text()
            cambio_meses = self.diccionario_meses()
            if tipo_boletin == 'BOE':
                fechaAux = fecha.split(' ')
                if len(fechaAux[1]) == 1:
                    fechaAux[1] = '0' + fechaAux[1]
                fecha = fechaAux[1] + '/' + cambio_meses[fechaAux[3].lower()] + '/' + fechaAux[5]
            elif tipo_boletin == 'BOPT':
                fechaAux = fecha.split(' ')
                if len(fechaAux[0]) == 1:
                    fechaAux[0] = '0' + fechaAux[0]
                fecha = fechaAux[0] + '/' + cambio_meses[fechaAux[2].lower()] + '/' + fechaAux[4]
            elif tipo_boletin == 'BOPH' or tipo_boletin == 'BOPZ':
                fechaAux = fecha.split(' ')
                if len(fechaAux[0]) == 1:
                    fechaAux[0] = '0' + fechaAux[0]
                fecha = fechaAux[0] + '/' + cambio_meses[fechaAux[1].lower()] + '/' + fechaAux[2]

            if tipo_boletin == 'BOPT':
                texto = 'Núm. ' + texto.split('Núm. ')[1]

            try:
                fp = open(output_filepath, 'w+', encoding='utf-8')
                if legible:
                    texto = textwrap3.fill(texto, width=500)
                
                fp.write(texto)
                fp.close()
                span.set_attribute('output_file_written', True)
            except Exception as e:
                msg = f"Failed to write output text file: {output_filepath}"
                self.logger.exception(msg)
                span.set_attribute('output_file_written', False)
                span.record_exception(e)
                raise OutputTextError(msg) from e
