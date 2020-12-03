# Nombre: pdf_a_txt.py
# Autor: Oscar Potrony / Georvic Tur
# Fecha: 14/09/2020
# Descripción: Lee el boletín indicado y lo guarda como txt en la ruta indicada.
# Invocación:
#	python pdf_a_txt.py folder_pdf folder_txt
# Ejemplo invocación:
#	python .\tools\pdf_to_text\pdf_a_txt.py .\data\raw\20200930\pdf\BOE_20200930_1.pdf .\data\extracted\20200930\txt\BOE_20200930_1.txt


import os
import re
import sys
import logging

from pathlib import Path
from xml.etree import ElementTree as ET

import pdfplumber
import textwrap3

logger = logging.getLogger('pdf_a_text')
logging.basicConfig()

# create file handler which logs even debug messages
# fh = logging.FileHandler('pdf_a_text.log')
# fh.setLevel(logging.INFO)

# create console handler with a higher log level
ch = logging.StreamHandler(sys.stdout)
ch.setLevel(logging.DEBUG)
logger.addHandler(ch)


class PDFError(Exception):
    pass


class OutputTextError(Exception):
    pass

MAX_CHARS_PER_LINE = 500
TIPO_BOLETINES = ['BOE', 'BOA', 'BOPH', 'BOPZ', 'BOPT']

# Concatena las partes de las palabras que quedan cortadas por fin de línea física (indicado con -).
# Parámetro extra si se quieren subir a la línea superior también los puntos y los dos puntos.
def quitar_guion_fin_renglon(texto, extra=True):
    if extra:
        return re.sub(r'([a-zA-Z0-9áéíóúÁÉÍÓÚñÑ]+)-\n([a-zA-Z0-9áéíóúÁÉÍÓÚñÑ,;.:)]+)', r'\1\2\n',texto)
    else:
        return re.sub(r'([a-zA-Z0-9áéíóúÁÉÍÓÚñÑ]+)-\n([a-zA-Z0-9áéíóúÁÉÍÓÚñÑ,;)]+)', r'\1\2\n',texto)


# Quita los espacios en blanco tras un punto (o dos puntos) y aparte.
def quitar_blankspaces_finales(texto):
    return re.sub(r'([.:])[ ]+\n',r'\1\n', texto)


# Juntar por párrafos, a excepción de títulos (por si terminan en espacio).
def juntar_por_parrafos(texto):
    return re.sub(r'([^A-Z]) \n|([^A-Z])\n ', r'\1\2 ', texto)


# Juntar por párrafos, identificándolos en función de su punto y aparte (o dos puntos).
def juntar_por_parrafos_punto(texto):
    texto = re.sub(r'([^.:])\n', r'\1 ', texto)
    return re.sub('[ ]+',' ', texto)

def diccionario_meses():
    ruta_fcs = Path(__file__).parent.parent / 'ficheros_configuracion'
    ruta_fichero_aux= ruta_fcs / 'auxiliar.xml'
    try:
        with open(ruta_fichero_aux, 'rb') as file:
            tree_aux = ET.parse(file)
            root_aux = tree_aux.getroot()
    except:
        msg = (
            "\nFailed: Open {ruta_fichero_aux}"
        ).format(
            ruta_fichero_aux=ruta_fichero_aux
        )
        logger.exception(
            msg
        )
        sys.exit()

    out = {}
    iter = root_aux.find('./correspondencias_meses').iter()
    next(iter)
    for t in iter:
        out[t.tag] = t.text

    return out

# Devuelve el elemento root del fichero de configuración correspondiente a tipo_boletin
def recuperar_fichero_configuracion(tipo_boletin):
    ruta_fcs = Path(__file__).parent.parent / 'ficheros_configuracion'
    ruta_fichero_conf = ruta_fcs / (tipo_boletin + '_conf.xml')
    if not ruta_fichero_conf.exists():
        return None
    try:
        with open(ruta_fichero_conf, 'rb') as file:
            tree = ET.parse(file)
            root = tree.getroot()
    except:
        msg = (
            "\nFailed: Open {ruta_fichero_conf}"
        ).format(
            ruta_fichero_conf=ruta_fichero_conf
        )
        logger.exception(
            msg
        )
        return
    return root

def from_pdf_to_text(
        input_filepath: str,
        output_filepath: str,
        tipo_boletin: dict,
        legible: bool=False
        ):

    try:
        pdf = pdfplumber.open(input_filepath)
    except:
        msg = (
            "\nFailed: Read {path}"
        ).format(
            path=input_filepath
        )
        logger.exception(
            msg
        )
        sys.exit()

    # Intentar obtener fichero de configuración del tipo_boletin.
    root_fc = recuperar_fichero_configuracion(tipo_boletin)
    if root_fc is not None:
        bounding_box = (int(root_fc.find('./puntos_corte/x0').text),
                        int(root_fc.find('./puntos_corte/top').text),
                        int(root_fc.find('./puntos_corte/x1').text),
                        int(root_fc.find('./puntos_corte/bottom').text))
        bounding_box_fecha = (
            int(root_fc.find('./puntos_corte/x0_fecha').text),
            int(root_fc.find('./puntos_corte/top_fecha').text),
            int(root_fc.find('./puntos_corte/x1_fecha').text),
            int(root_fc.find('./puntos_corte/bottom_fecha').text)
        )
    else:			# Si el boletín no está en la configuración, se utilizan valores por defecto (similares a los del BOE)
        bounding_box = (50, 100, 500, 800)
        bounding_box_fecha = (150, 75, 400, 90)

    # Recuperar el texto necesario
    texto = ''
    for page in pdf.pages:
        # withinBBoxPage = page.within_bbox(bounding_box)		# Alternativa.
        try:
            croppedPage = page.crop(bounding_box)
            extracted_text = croppedPage.extract_text()
            if extracted_text is not None:
                texto += extracted_text + '\n'   # \n necesario por las páginas que terminan con media palabra.
        except:
            msg = (
                "\nWarning: Failed: Read page from {path}"
            ).format(
                path=input_filepath
            )
            logger.exception(
                msg
            )
            # No invalida el documento porque suele ocurrir por páginas apaisadas, que se corresponden
            # a tablas de puestos de anexos, que se tratarán independientemente del texto.

    # Dar formato correcto al texto
    if tipo_boletin == 'BOPT':
        texto = quitar_blankspaces_finales(texto) # Preprocesamiento del BOPT

    if tipo_boletin in ['BOE', 'BOA', 'BOPZ', 'BOPT']:
        texto = quitar_guion_fin_renglon(texto)
        texto = juntar_por_parrafos(texto)
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

        string_texto = juntar_por_parrafos_punto(
            quitar_guion_fin_renglon(string_texto, False))

        texto = string_inicial + '\n' + string_texto
    else:
        # Para otro tipo de boletines, se realiza esto como procesamiento único, además del corte previo.
        texto = quitar_guion_fin_renglon(texto)
        texto = juntar_por_parrafos(texto)

    ## Recuperar fecha
    page = pdf.pages[0]
    fecha = page.crop(bounding_box_fecha).extract_text()

    # Dar formato correcto a la fecha (BOA es correcto de forma predeterminada, otros boletines se dan por correctos.)
    cambio_meses = diccionario_meses()
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

    # Escribir fecha y texto
    if tipo_boletin == 'BOPT':
        # Quedarse solo con el primer artículo que aparece (sin contar el incompleto), ya que los otros,
        # en caso de ser de empleo volverán a salir. Así se evitan duplicados y problemas con las info.
        texto = 'Núm. ' + texto.split('Núm. ')[1]

    try:
        fp = open(output_filepath, 'w+', encoding='utf-8')
        if legible:
            texto = textwrap3.fill(texto, width=MAX_CHARS_PER_LINE)
        
        fp.write(texto)
        fp.close()
    except Exception as e:
        msg = (
            "No se ha podido escribir el"
            f" texto en el fichero {output_filepath}."
        )
        raise OutputTextError(msg) from e


def main(overwrite=True):

    if len(sys.argv) != 3:
        print('Numero de parametros incorrecto.')
        sys.exit()

    input_folder = sys.argv[1]
    output_folder = sys.argv[2]

    path = Path(input_folder)
    for p in sorted(path.rglob("*")):
        input_filepath = str(p)
        if ".pdf" not in input_filepath:
            continue
        match_tipo_boletin = re.search(
            r"{}".format("("+"|".join(TIPO_BOLETINES)+")"),
            input_filepath
        )
        if match_tipo_boletin is not None:
            tipo_boletin = match_tipo_boletin.group(1)
            output_filepath = input_filepath.replace(
                input_folder.replace("."+os.sep, ""),
                output_folder
            ).replace(
                ".pdf",
                ".txt"
            )
            msg = (
                "\nProcessing: {input_filepath} as "
                "{tipo_boletin} in \n{output_filepath}"
            ).format(
                input_filepath=input_filepath,
                tipo_boletin=tipo_boletin,
                output_filepath=output_filepath
            )
            logger.info(msg)

            # Create an empty output file along with all of its parent
            # folders if needed.
            path = Path(output_filepath)
            if not path.exists():
                # if you want to overwrite
                path.parent.mkdir(exist_ok=True, parents=True)
                path.touch(exist_ok=True)
            elif path.exists() and not overwrite:
                # if you want to skip the extracted files.
                logger.info(
                    f"Skipping {input_filepath} "
                    f"because {output_filepath} already exists"
                )
                continue

            try:
                from_pdf_to_text(
                    input_filepath=input_filepath,
                    output_filepath=output_filepath,
                    tipo_boletin=tipo_boletin
                )
            except (PDFError, OutputTextError):
                msg = (
                    "\nFailed: {input_filepath} as "
                    "{tipo_boletin} in \n{output_filepath}"
                ).format(
                    input_filepath=input_filepath,
                    tipo_boletin=tipo_boletin,
                    output_filepath=output_filepath
                )
                logger.exception(
                    msg
                )
            except (AttributeError, KeyError):
                msg = (
                    "\nFailed: {input_filepath} as "
                    "{tipo_boletin} in \n{output_filepath}. "
                    "It is possible that a field could not "
                    "be extracted from the PDF."
                ).format(
                    input_filepath=input_filepath,
                    tipo_boletin=tipo_boletin,
                    output_filepath=output_filepath
                )
                logger.exception(
                    msg
                )


if __name__ == "__main__":
    main(overwrite=True)
