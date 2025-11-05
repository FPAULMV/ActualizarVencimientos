import versioning, os, urllib.parse, sys
from pathlib import Path
from sqlalchemy import create_engine, text
from pandas import DataFrame
from dotenv import load_dotenv




try:
    load_dotenv()
    PATH_FILES_SINERGIA = os.getenv('PATH_FILES_SINERGIA')
    PATH_FILES_PETRODIESEL = os.getenv('PATH_FILES_PETRODIESEL')
    PATH_FILES_PROENERGETICS = os.getenv('PATH_FILES_PROENERGETICS')
    CONN_STR = os.getenv('CONN_STR')
    odbc_encoded = urllib.parse.quote_plus(CONN_STR)
    engine = create_engine(f"mssql+pyodbc:///?odbc_connect={odbc_encoded}",pool_pre_ping=True,fast_executemany=True)
    print("\n--- Variables de entorno cargadas con exito. ---\n")
    
except Exception as e:
    # Agregar Log.Error
    print("Hubo un error al cargar las variables de entorno.")
    print(e)
    sys.exit("-> Fin de la ejecucion del programa. <-".upper())
finally:
    #Agregar Log.info
    print("-> FIN: de la carga de las variables de entorno.\n")



class Vencimientos():
    def __init__(self, query: str, ruta_documentos: Path):
        self.consulta_vencimientos = text(query)
        self.ruta_documentos = Path(ruta_documentos)

    def vencimientos(self):
        pass


    def clientes(self):
        pass
    

    def portal_id_clientes(self):
        pass


    def crear_documentos(self):
        pass


    def cardsystem_insert(self):
        pass


    def exec_insert(self):
        pass