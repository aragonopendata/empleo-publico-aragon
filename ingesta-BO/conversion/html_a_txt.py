import pathlib
import codecs
from bs4 import BeautifulSoup
import textwrap3
from xml.etree import ElementTree as ET

class HtmlToTextConverter:
    MAX_CHARS_PER_LINE = 500

    def __init__(self, tracer, logger):
        self.tracer = tracer
        self.logger = logger

    def from_html_to_text(self, input_filepath, output_filepath, tipo_boletin, legible):
        """
        Convierte un archivo HTML a texto plano según las configuraciones especificadas para cada boletín.
        
        Args:
        - input_filepath: Ruta del archivo HTML de entrada.
        - output_filepath: Ruta del archivo de texto de salida.
        - tipo_boletin: Tipo de boletín que se está procesando (BOA, BOPH, BOPZ, BOPT, BOE).
        - legible: Booleano que indica si el texto debe formatearse para legibilidad.
        """
        with self.tracer.start_as_current_span('from_html_to_text') as span:
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
                    self.logger.info(f"Configuration file loaded successfully: {ruta_fichero_conf}")
                    span.set_attribute('config_file_loaded', True)
            except Exception as e:
                msg = f"Failed to open configuration file: {ruta_fichero_conf}"
                self.logger.exception(msg)
                span.set_attribute('config_file_loaded', False)
                span.record_exception(e)
                return

            try:
                with codecs.open(input_filepath, 'r', root_fc.find('./charsets/html').text) as file:
                    html = file.read()
                    self.logger.info(f"Input HTML file read successfully: {input_filepath}")
                    span.set_attribute('html_file_read', True)
            except Exception as e:
                msg = f"Failed to open input HTML file: {input_filepath}"
                self.logger.exception(msg)
                span.set_attribute('html_file_read', False)
                span.record_exception(e)
                return

            # Parse HTML and extract text
            soup = BeautifulSoup(html, 'html.parser')
            texto_html = soup.get_text()

            if tipo_boletin in ['BOA', 'BOPH', 'BOPZ', 'BOPT']:
                inicio_texto = 'Texto completo:'
                fin_texto = '\n\n\n\n\n\n\n\n\n\n'
            elif tipo_boletin == 'BOE':
                inicio_texto = 'TEXTO ORIGINAL'
                fin_texto = '\nsubir\n'

            if tipo_boletin in ['BOA', 'BOPH', 'BOPZ', 'BOPT', 'BOE']:
                texto_html = texto_html[texto_html.find(inicio_texto):][len(inicio_texto):]
                texto_html = texto_html[:texto_html.find(fin_texto)]
                span.set_attribute('text_extracted', True)
            else:
                span.set_attribute('text_extracted', False)

            # Quitar primeros saltos de línea
            num_saltos = 0
            i = 0
            while i < len(texto_html):
                if texto_html[i] == '\n' or texto_html[i] == ' ':
                    num_saltos += 1
                else:
                    break
                i += 1

            texto_html = texto_html[num_saltos:]

            # Escribir fichero
            try:
                fp = open(output_filepath, 'w+', encoding='utf-8')
                if legible:
                    texto_html = textwrap3.fill(texto_html, width=500)
                
                fp.write(texto_html)
                fp.close()
                self.logger.info(f"Output text file written successfully: {output_filepath}")
                span.set_attribute('output_file_written', True)
            except Exception as e:
                msg = f"Failed to write output text file: {output_filepath}"
                self.logger.exception(msg)
                span.set_attribute('output_file_written', False)
                span.record_exception(e)
