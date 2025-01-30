# [SYSTEM]
import sys
import os
from pathlib import Path
from xml.etree import ElementTree as ET
import json

# [CLASES]
from ingesta_boe import IngestaBOE
from ingesta_aragon import IngestaAragon
from ingesta_extra import IngestaExtra

# [TRACER Y LOGGER]
from opentelemetry import trace

sys.path.append(os.path.abspath('/app/ingesta-BO'))
from tracer.tracer_configurator import TracerConfigurator
from logger.logger_configurator import LoggerConfigurator

dag_id = sys.argv[-1]

tracer_configurator = TracerConfigurator(dag_id=dag_id)
tracer = tracer_configurator.get_tracer()

logger_configurator = LoggerConfigurator(name='Ingesta', dag_id=dag_id)
logger = logger_configurator.get_logger()

ingesta_boe = IngestaBOE(tracer, logger)
ingesta_aragon = IngestaAragon(tracer, logger)
ingesta_extra = IngestaExtra(tracer, logger)

# Función para recuperar fichero de configuración
def recuperar_fichero_configuracion(ruta_fcs, tipo_boletin):
    with tracer.start_as_current_span("Recuperar fichero configuración") as span:
        ruta_fichero_conf = ruta_fcs / (tipo_boletin + '_conf.xml')
        span.set_attribute("file.path", str(ruta_fichero_conf))
        try:
            with open(ruta_fichero_conf, 'rb') as file:
                tree = ET.parse(file)
                root = tree.getroot()
                logger.info(f"Archivo de configuración leído exitosamente: {ruta_fichero_conf}")
                span.set_attribute("config.leido", True)
                return root
        except Exception as e:
            msg = f"Failed to open {ruta_fichero_conf}"
            logger.exception(msg)
            span.record_exception(e)
            span.set_status(trace.status.StatusCode.ERROR)
            return None

def ingesta_diaria(dia, directorio_base):
    with tracer.start_as_current_span("Ingesta diaria") as span_main:
        logger.info(f"Iniciando ingesta diaria para el día {dia} en el directorio {directorio_base}")
        span_main.set_attribute("dia", dia)
        span_main.set_attribute("directorio_base", str(directorio_base))

        # Crear directorio del día si no está creado
        with tracer.start_as_current_span("Crear directorio del día") as span_create_dir:
            try:
                path = directorio_base / dia
                if not path.exists():
                    path.mkdir()
                    logger.info(f"Directorio creado: {path}")
                    span_create_dir.set_attribute("directorio_creado", str(path))
            except Exception as e:
                msg = f"Failed to create directory {path}"
                logger.exception(msg)
                span_create_dir.record_exception(e)
                span_create_dir.set_status(trace.status.StatusCode.ERROR)

        # Leer el fichero de configuración del BOE para obtener formatos
        ruta_fcs = Path(__file__).absolute().parent.parent / 'ficheros_configuracion'
        ruta_fichero_conf = ruta_fcs / 'BOE_conf.xml'
        with tracer.start_as_current_span("Leer configuración BOE") as span_read_config:
            try:
                with open(ruta_fichero_conf, 'rb') as file:
                    tree_fc = ET.parse(file)
                    root_fc = tree_fc.getroot()
                    logger.info(f"Archivo de configuración del BOE leído exitosamente: {ruta_fichero_conf}")
                    span_read_config.set_attribute("config.path", str(ruta_fichero_conf))
            except Exception as e:
                msg = f"Failed to open {ruta_fichero_conf}"
                logger.exception(msg)
                span_read_config.record_exception(e)
                span_read_config.set_status(trace.status.StatusCode.ERROR)
                return

        formatos = []
        for t in root_fc.find('url_formatos').iter():
            formatos.append(t.tag)
        formatos = formatos[1:]

        # Crear directorios de formato si no están creados
        for tipo_articulo in ['apertura', 'cierre']:
            with tracer.start_as_current_span(f"Crear directorios para {tipo_articulo}") as span_create_dirs:
                # Crear directorios de tipos si no están creados
                try:
                    path = directorio_base / dia / tipo_articulo
                    if not path.exists():
                        path.mkdir()
                        logger.info(f"Directorio creado: {path}")
                        span_create_dirs.set_attribute("directorio_tipo_creado", str(path))
                except Exception as e:
                    msg = f"Failed to create directory {path}"
                    logger.exception(msg)
                    span_create_dirs.record_exception(e)
                    span_create_dirs.set_status(trace.status.StatusCode.ERROR)

                try:
                    for formato in formatos:
                        path = directorio_base / dia / tipo_articulo / formato
                        if not path.exists():
                            path.mkdir()
                            logger.info(f"Directorio creado: {path}")
                            span_create_dirs.set_attribute(f"directorio_{formato}_creado", str(path))
                        if formato.lower() == 'pdf':  # Crear directorio de pdfs rotados
                            path_rotados = directorio_base / dia / tipo_articulo / formato / 'rotados'
                            if not path_rotados.exists():
                                path_rotados.mkdir()
                                logger.info(f"Directorio creado: {path_rotados}")
                                span_create_dirs.set_attribute("directorio_rotados_creado", str(path_rotados))
                except Exception as e:
                    msg = f"Failed to create directory {path}"
                    logger.exception(msg)
                    span_create_dirs.record_exception(e)
                    span_create_dirs.set_status(trace.status.StatusCode.ERROR)

                # Crear directorio de info si no está creado
                try:
                    path_info = directorio_base / dia / tipo_articulo / 'info/'
                    if not path_info.exists():
                        path_info.mkdir()
                        logger.info(f"Directorio creado: {path_info}")
                        span_create_dirs.set_attribute("directorio_info_creado", str(path_info))
                except Exception as e:
                    msg = f"Failed to create directory {path_info}"
                    logger.exception(msg)
                    span_create_dirs.record_exception(e)
                    span_create_dirs.set_status(trace.status.StatusCode.ERROR)

        # Comprobar de qué boletines se tienen ficheros de configuración
        boletines = []
        for x in os.listdir(ruta_fcs):
            if x.endswith('_conf.xml'):
                boletines.append(x.split('_')[0])
        span_main.set_attribute("boletines_encontrados", boletines)

        for tipo_boletin in boletines:
            with tracer.start_as_current_span(f"Ingesta {tipo_boletin}") as span_ingesta:
                if tipo_boletin == 'BOE':  # Realizar la ingesta del BOE
                    try:
                        ingesta_boe.ingesta_diaria_boe(dia, directorio_base)
                        logger.info(f"Ingesta BOE completada para el día {dia}")
                    except Exception as e:
                        logger.error(f"Error al realizar la ingesta del BOE para el día {dia}")
                        span_ingesta.record_exception(e)
                        span_ingesta.set_status(trace.status.StatusCode.ERROR)
                        fichero_log = Path(__file__).absolute().parent.parent / 'articulos_erroneos.log'
                        with open(fichero_log, 'a') as file:
                            file.write(f'{dia} - INGESTA BOE\n')

                elif tipo_boletin in ['BOA', 'BOPH', 'BOPZ', 'BOPT']:  # Realizar la ingesta de boletines provinciales aragoneses
                    try:
                        config_root = recuperar_fichero_configuracion(ruta_fcs, tipo_boletin)
                        if config_root is not None:
                            ingesta_aragon.ingesta_diaria_aragon_por_tipo(dia, directorio_base, tipo_boletin, config_root)
                            logger.info(f"Ingesta {tipo_boletin} completada para el día {dia}")
                        else:
                            logger.warning(f"No se pudo recuperar la configuración para el boletín {tipo_boletin}")
                    except Exception as e:
                        logger.error(f"Error al realizar la ingesta de Aragón para el boletín {tipo_boletin} para el día {dia}")
                        span_ingesta.record_exception(e)
                        span_ingesta.set_status(trace.status.StatusCode.ERROR)
                        fichero_log = Path(__file__).absolute().parent.parent / 'articulos_erroneos.log'
                        with open(fichero_log, 'a') as file:
                            file.write(f'{dia} - INGESTA ARAGÓN\n')
                else:
                    try:
                        ingesta_extra.ingesta_diaria_extra(dia, directorio_base, tipo_boletin)
                        logger.info(f"Ingesta {tipo_boletin} completada para el día {dia}")
                    except Exception as e:
                        logger.error(f"Error al realizar la ingesta extra para el boletín {tipo_boletin} para el día {dia}")
                        span_ingesta.record_exception(e)
                        span_ingesta.set_status(trace.status.StatusCode.ERROR)
                        fichero_log = Path(__file__).absolute().parent.parent / 'articulos_erroneos.log'
                        with open(fichero_log, 'a') as file:
                            file.write(f'{dia} - INGESTA EXTRA\n')

def main():
    if len(sys.argv) != 4:
        logger.error('Numero de parametros incorrecto.')
        return

    dia = sys.argv[1]
    directorio_base = Path(sys.argv[2])

    try:
        with tracer.start_as_current_span("Ingesta main"):
            logger.info(f"Iniciando ingesta diaria para el día {dia} en el directorio {directorio_base}")
            ingesta_diaria(dia, directorio_base)
    finally:
        #detach(token)
        pass
if __name__ == "__main__":
    main()
