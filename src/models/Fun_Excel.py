
import logging
import xlwings as xw
import os
from datetime import datetime, timedelta
import pandas as pd
from pathlib import Path



fecha_actual = datetime.now()
fecha_ayer = fecha_actual - timedelta(days=1)
año = fecha_ayer.strftime('%Y')
mes = fecha_ayer.strftime('%m')
dia = fecha_ayer.strftime('%d')

     

def Eliminar_Excel(ruta_libro):
    if os.path.exists(ruta_libro):
        os.remove(ruta_libro)
        logging.info(f"El archivo {ruta_libro} se ha eliminado con éxito.")
    else:
        logging.info(f"El archivo {ruta_libro} no existe.")

