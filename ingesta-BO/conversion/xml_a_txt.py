import logging
import re
from xml.etree import ElementTree as ET
import pathlib
import textwrap3


class XMLError(Exception):
    pass


class OutputTextError(Exception):
    pass


class XMLToTextConverter:
    MAX_CHARS_PER_LINE = 500

    def __init__(self, tracer, logger):
        self.tracer = tracer
        self.logger = logger

    def from_xml_to_text(self, input_filepath, output_filepath, tipo_boletin, legible=False):
        with self.tracer.start_as_current_span('from_xml_to_text') as span:
            span.set_attribute('input_filepath', str(input_filepath))
            span.set_attribute('output_filepath', str(output_filepath))
            span.set_attribute('tipo_boletin', tipo_boletin)
            span.set_attribute('legible', legible)

            # Recuperar fichero de configuración
            ruta_fcs = pathlib.Path(__file__).absolute().parent.parent / 'ficheros_configuracion'
            ruta_fichero_conf = ruta_fcs / (tipo_boletin + '_conf.xml')

            try:
                with open(ruta_fichero_conf, 'rb') as file:
                    tree_fc = ET.parse(file)
                    root_fc = tree_fc.getroot()
                    span.set_attribute('config_file_loaded', True)
            except Exception as e:
                msg = f"Failed to open configuration file: {ruta_fichero_conf}"
                self.logger.exception(msg)
                span.set_attribute('config_file_loaded', False)
                span.record_exception(e)
                raise XMLError(msg) from e

            # Abrir el XML
            try:
                with open(input_filepath, 'rb') as file:
                    tree = ET.parse(file)
                    root = tree.getroot()
                    span.set_attribute('xml_file_opened', True)
            except Exception as e:
                msg = f"Failed to read XML file: {input_filepath}"
                self.logger.exception(msg)
                span.set_attribute('xml_file_opened', False)
                span.record_exception(e)
                raise XMLError(msg) from e

            # Obtener texto
            try:
                if tipo_boletin == 'BOE':
                    texto = ''
                    for parrafo in root.findall(root_fc.find('./etiquetas_xml/auxiliares/texto').text):
                        texto += parrafo.text + '\n'
                else:
                    texto = root.find(root_fc.find('./etiquetas_xml/auxiliares/texto').text).text
                span.set_attribute('text_extracted', True)
            except Exception as e:
                msg = "Failed to extract text from XML"
                self.logger.exception(msg)
                span.set_attribute('text_extracted', False)
                span.record_exception(e)
                raise XMLError(msg) from e

            # Escribir fichero
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