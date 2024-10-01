import logging
from pathlib import Path
from datetime import date
import sys
import os

class LoggerConfigurator:
    def __init__(self, name: str, dag_id: str, log_dir: str = '/opt/airflow/ingesta-BO/logs', level: int = logging.DEBUG):
        self.name = name
        self.dag_id = dag_id
        self.log_dir = log_dir
        self.level = level
        self.logger = self._setup_logger()

    def _setup_logger(self):
        # Crear un logger
        logger = logging.getLogger(self.name)
        logger.setLevel(self.level)

        # Crear el directorio de logs si no existe
        log_date = date.today().strftime('%Y-%m-%d')
        log_path = Path(f"{self.log_dir}/{self.dag_id}/{log_date}")
        log_path.mkdir(parents=True, exist_ok=True)

        # Crear un handler para escribir logs a un archivo
        log_file = log_path / f"{self.name}.log"
        fh = logging.FileHandler(log_file)
        fh.setLevel(self.level)

        # Crear un formateador y añadirlo al handler
        formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
        fh.setFormatter(formatter)

        # Añadir el handler al logger
        logger.addHandler(fh)

        # Crear un handler para mostrar logs en la consola
        ch = logging.StreamHandler(sys.stdout)
        ch.setLevel(self.level)
        ch.setFormatter(formatter)
        logger.addHandler(ch)

        logger.disabled = False

        return logger

    def get_logger(self):
        return self.logger
