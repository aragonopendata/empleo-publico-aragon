import os
import shutil
import sys
import logging
from datetime import date
from pathlib import Path

# [TRACER Y LOGGER]
from opentelemetry import trace

sys.path.append(os.path.abspath('/opt/airflow/ingesta-BO'))
from tracer.tracer_configurator import TracerConfigurator
from logger.logger_configurator import LoggerConfigurator

dag_id = sys.argv[-1]

tracer_configurator = TracerConfigurator(service_name=f'Eliminar BO Task - {dag_id}', dag_id=dag_id)
tracer = tracer_configurator.get_tracer()

logger_configurator = LoggerConfigurator(name='Eliminar BO', dag_id=dag_id)
logger = logger_configurator.get_logger()

def delete_old_boletines(base_path, n_regs, logger, tracer):
    """Elimina boletines antiguos del sistema de archivos, manteniendo los N_REGS más recientes."""
    base_folder = Path(base_path) / 'ingesta-BO' / 'data'
    logger.info(f'Revisando boletines en: {base_folder}')

    try:
        with tracer.start_as_current_span("Delete Old Boletines") as span:
            boletines = sorted(base_folder.iterdir(), reverse=True)
            logger.debug(f'Boletines encontrados: {boletines}')

            if len(boletines) <= n_regs:
                logger.info('No hay suficientes boletines antiguos para eliminar.')
                span.set_status(trace.StatusCode.OK)
                return

            for bo in boletines[n_regs:]:
                if bo.is_dir():
                    shutil.rmtree(bo)
                    logger.info(f'Eliminado: {bo}')
                    span.add_event(f'Eliminado: {bo}')

            span.set_status(trace.StatusCode.OK)
    except Exception as e:
        logger.exception("Error al eliminar boletines")
        span.record_exception(e)
        span.set_status(trace.StatusCode.ERROR)
        raise

def main():
    """Función principal para ejecutar la eliminación de boletines."""
    if len(sys.argv) != 2:
        logger.error("Uso: python eliminar_BO.py <dag_id>")
        sys.exit(1)

    BASE_PATH = "/opt/airflow"
    N_REGS = 10

    delete_old_boletines(BASE_PATH, N_REGS, logger, tracer)

if __name__ == "__main__":
    main()
