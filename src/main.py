import versioning, os, urllib.parse, sys, pandas
from pathlib import Path
from sqlalchemy import create_engine, text
from pandas import DataFrame
from dotenv import load_dotenv



class Vencimientos():
    def __init__(self, query: str, output_documentos: Path):
        self.__QUERY_VENCIMIENTOS = text(query)
        self.__OUTPUT_DOCUMENTOS = Path(output_documentos)
        self.__CAMPOS_REQUERIDOS = [
            "Destino",
            "NumeroFactura",
            "FechaFactura",
            "Vencimiento",
            "Total",
            "Saldo",
            "ReferenciaBancaria",
            "DiasCredito", 
            "DiasVencido"
        ]
        self.DF_VENCIMIENTOS =  self.obtener_vencimientos(self.__QUERY_VENCIMIENTOS)
        self.validar_campos(self.DF_VENCIMIENTOS, self.__CAMPOS_REQUERIDOS)
        self.LISTA_CLIENTES = self.clientes(self.DF_VENCIMIENTOS)
        


    def obtener_vencimientos(self, query: str) -> DataFrame:
        try:
            with engine.connect() as conn:
                df = pandas.read_sql(query, conn)
                if df.empty:
                    print("La consulta devolvio un DataFrame vacio.")
                    sys.exit("-> Fin de la ejecucion del programa. <-".upper())
                    
                return df
        except Exception as e:
            print(e)
        finally:
            print("-> FIN: Termino la consulta de los vencimientos.\n")

    def validar_campos(self, data_frame: DataFrame, campos: list) -> bool:
        try:
            value = False
            campos_formateados = {str(c).upper().strip().replace(" ", "").replace("_", "") for c in campos}
            df_columnas = {str(c).upper().strip().replace(" ", "").replace("_", "") for c in data_frame.columns}
            faltantes =  campos_formateados - df_columnas
            if faltantes:
                print(f"Faltan columnas en el DataFrame {sorted(faltantes)}")
                sys.exit("-> Fin de la ejecucion del programa. <-".upper())

            value = True
            return value
        except Exception as e:
            print("ERROR: Ocurrio un error al comprobar los campos del DataFrame")
            print(e)
        finally:
            print("-> FIN: Termino la validacion de las columnas del DataFrame.\n")


    def clientes(self, data_frame: DataFrame) -> list:
        try:    
            clientes = data_frame["Destinos"].dropna().unique().tolist()
            if not clientes:
                print("INFO: No hay clientes para validar en el DataFrame.")
                sys.exit("-> Fin de la ejecucion del programa. <-".upper())
            return clientes
        except Exception as e:
            print("ERROR: Ocurrio un error al obtener la lista de clientes unicos.")
            print(e)
            sys.exit("-> Fin de la ejecucion del programa. <-".upper())
        finally:
            print("-> FIN: Termino de obtener la lista de los clientes unicos.\n")

    def portal_id_clientes(self):
        
        pass


    def crear_documentos(self):
        pass


    def cardsystem_insert(self):
        pass


    def exec_insert(self):
        pass




try:
    load_dotenv()
    PATH_FILES_SINERGIA = os.getenv('PATH_FILES_SINERGIA')
    QUERY_SINERGIA = os.getenv('QUERY_SINERGIA')

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




if __name__ == '__main__':
    
    sinergia = Vencimientos(QUERY_SINERGIA, PATH_FILES_SINERGIA)
    print(sinergia.MIS_VENCIMIENTOS)
    sinergia
