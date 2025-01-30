import sys
import os
import logging
from datetime import date

from pathlib import Path
from xml.etree import ElementTree as ET

# [CLASES]
from pdf_a_txt import PDFToTextConverter
from xml_a_txt import XMLToTextConverter
from html_a_txt import HtmlToTextConverter

# [TRACER Y LOGGER]
from opentelemetry import trace

sys.path.append(os.path.abspath('/app/ingesta-BO'))
from tracer.tracer_configurator import TracerConfigurator
from logger.logger_configurator import LoggerConfigurator

dag_id = sys.argv[-1]

logger_configurator = LoggerConfigurator(name='Conversión', dag_id=dag_id)
logger = logger_configurator.get_logger()

tracer_configurator = TracerConfigurator(dag_id=dag_id)
tracer = tracer_configurator.get_tracer()

pdf_a_txt = PDFToTextConverter(tracer, logger)
xml_a_txt = XMLToTextConverter(tracer, logger)
html_a_txt = HtmlToTextConverter(tracer, logger)


def conversion_a_texto(directorio_base, ruta_auxiliar, legible=False):

    with tracer.start_as_current_span("Leer archivo auxiliar") as span:
        try:
            with open(ruta_auxiliar, 'rb') as file:
                tree_auxiliar = ET.parse(file)
                root_auxiliar = tree_auxiliar.getroot()
                logger.info(f"Archivo auxiliar leído exitosamente: {ruta_auxiliar}")
        except Exception as e:
            msg = f"\nFailed: Open {ruta_auxiliar}"
            logger.exception(msg)
            span.set_status(trace.StatusCode.ERROR)
            raise

    with tracer.start_as_current_span("Crear directorio de texto") as span:
        try:
            for tipo_articulo in ['apertura', 'cierre']:
                path_txt = directorio_base / tipo_articulo / 'txt'
                if not path_txt.exists():
                    path_txt.mkdir()
                    logger.info(f"Directorio creado: {path_txt}")
                else:
                    logger.info(f"El directorio ya existe: {path_txt}")
        except Exception as e:
            msg = f"\nFailed: Create {path_txt}"
            logger.exception(msg)
            span.set_status(trace.StatusCode.ERROR)
            raise
            
        legible_sufijo = ''
        if legible:
            legible_sufijo = '_legible'
            
    with tracer.start_as_current_span("Convertir archivos a texto") as span:
        span.set_attribute("conversion.legible", legible)
        for tipo_articulo in ['apertura', 'cierre']:
            for filename in os.listdir(directorio_base / tipo_articulo / 'pdf'):
                if filename != 'rotados':
                    tipo_boletin = filename.split('_')[0]
                    formato = root_auxiliar.find('./formatos_por_defecto/' + tipo_boletin).text
                    input_filepath = directorio_base / tipo_articulo / formato / (filename.split('.')[0] + '.' + formato)
                    output_filepath = directorio_base / tipo_articulo / 'txt' / (filename.split('.')[0] + legible_sufijo + '.txt')

                    try:
                        if formato == 'pdf':
                            pdf_a_txt.from_pdf_to_text(
                                input_filepath, output_filepath, tipo_boletin,
                                legible)
                        elif formato == 'xml':
                            xml_a_txt.from_xml_to_text(
                                input_filepath, output_filepath, tipo_boletin,
                                legible)
                        elif formato == 'html':
                            html_a_txt.from_html_to_text(
                                input_filepath, output_filepath, tipo_boletin,
                                legible)
                        logger.info(f"Archivo convertido exitosamente: {filename}")
                    except Exception as e:
                        logger.error(f"Error al convertir archivo {filename} en formato {formato}")
                        span.set_status(trace.StatusCode.ERROR)
                        
                        fichero_log = Path(__file__).absolute().parent.parent / 'articulos_erroneos.log'
                        with open(fichero_log, 'a') as file:
                            file.write(f'{filename.split(".")[0]} - CONVERSION\n')

def main():
    with tracer.start_as_current_span("Conversión a texto main") as span:
        if len(sys.argv) == 4:
            directorio_base = Path(sys.argv[1])
            ruta_auxiliar = Path(__file__).absolute().parent.parent / 'ficheros_configuracion' / 'auxiliar.xml'
            legible = False
        elif len(sys.argv) != 5:
            logger.error('Numero de parametros incorrecto.')
            span.set_status(trace.StatusCode.ERROR)
            return
        else:
            directorio_base = Path(sys.argv[1])
            ruta_auxiliar = Path(sys.argv[2])
            legible = sys.argv[3].lower() in ['true', 't', 'verdadero']

        conversion_a_texto(directorio_base, ruta_auxiliar, legible)

if __name__ == "__main__":
    main()