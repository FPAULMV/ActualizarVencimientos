import versioning, os, urllib.parse, sys, pandas
from pathlib import Path
from sqlalchemy import create_engine, text
from pandas import DataFrame
from dotenv import load_dotenv



class Vencimientos():
    def __init__(self):
        pass

    def obtener_vencimientos(self, query: str) -> DataFrame:
        try:
            query = text(query)
            with engine.connect() as conn:
                df = pandas.read_sql(query, conn)
                if df.empty:
                    print("La consulta devolvio un DataFrame vacio.")
                    sys.exit("-> Fin de la ejecucion del programa. <-".upper())
                    
                return df
        except Exception as e:
            print("ERROR: Hubo un error al consultar los vencimientos.")
            print(f"DETALLE: {e}")
            sys.exit("-> Fin de la ejecucion del programa. <-".upper())
        finally:
            print("-> FIN: Termino la consulta de los vencimientos.\n")

    def comprobar_columnas(self, data_frame: DataFrame, columnas: list = None) -> DataFrame:
        """
            Comprueba que un DataFrame contenga todas las columnas nombradas
            en una lista.
        """
        if columnas is None:
            columnas = ["Destino", "NumeroFactura", "FechaFactura",
                        "Vencimiento", "Total", "Saldo",
                        "ReferenciaBancaria", "DiasCredito", "DiasVencido",
                        "ClientId"]

        try:
            campos_formateados = {str(c).upper().strip().replace(" ", "").replace("_", "") for c in columnas}
            df_columnas = {str(c).upper().strip().replace(" ", "").replace("_", "") for c in data_frame.columns}
            faltantes =  campos_formateados - df_columnas
            if faltantes:
                print(f"Faltan columnas en el DataFrame {sorted(faltantes)}")
                sys.exit("-> Fin de la ejecucion del programa. <-".upper())

            df_filtrado = data_frame[['Destino', 'NumeroFactura', 'FechaFactura', 
                                      'Vencimiento', 'total', 'Saldo', 
                                      'ReferenciaBancaria', 'dias_credito', 'dias_vencido',
                                      'client_id']]
            
            return df_filtrado
        except Exception as e:
            print("ERROR: Ocurrio un error al comprobar los campos del DataFrame")
            print(e)
        finally:
            print("-> FIN: Termino la validacion de las columnas del DataFrame.\n")


    def clientes(self, data_frame: DataFrame) -> DataFrame:
        """
            Obtiene una lista con los clientes del DataFrame. 
            (O de lo que sea que incluya la columna 'Destinos.')
        """
        try:    
            clientes = data_frame[["Destino", "client_id"]].dropna().drop_duplicates(subset="Destino")
            if clientes.empty:
                print("INFO: No hay clientes para validar en el DataFrame.")
                sys.exit("-> Fin de la ejecucion del programa. <-".upper())
            return clientes
        except Exception as e:
            print("ERROR: Ocurrio un error al obtener la lista de clientes unicos.")
            print(e)
            sys.exit("-> Fin de la ejecucion del programa. <-".upper())
        finally:
            print("-> FIN: Termino de obtener la lista de los clientes unicos.\n")






class CrearDoumento():
    """Crea documentos a partir de un DataFrame."""

    def __init__(self, data_frame: DataFrame, carpeta_salida: Path):
        self.data_frame = data_frame
        self.carpeta_salida = Path(carpeta_salida)


    def crear_documentos_csv(self, nombre_archivo: str) -> None:
        """ Crea un documento '.csv' """
        try:
            ruta = Path(self.carpeta_salida / nombre_archivo)
            print(f"RUTA SALIDA: {ruta}")
            self.data_frame.to_csv(ruta, index= False, encoding= 'utf-8')
            return ruta
        except Exception as e:
            print(f"Hubo un error al crear el archivo. {ruta}")
            print(e)
        finally:
            (print("-> FIN: Termino la creacion del archivo."))


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
    
    sinergia = Vencimientos()
    df_completo = sinergia.obtener_vencimientos(QUERY_SINERGIA)
    df_vencimientos = sinergia.comprobar_columnas(df_completo)
    clientes = sinergia.clientes(df_vencimientos)
    
    CLIENTES_NO_ENCONTRADOS = []
    INSERTS_ARCHIVOS_GENERALES = []

    for _, row in clientes.iterrows():
        cliente = row['Destino']
        if cliente in df_vencimientos['Destino'].values:
            df_cliente = df_vencimientos[df_vencimientos['Destino'] == cliente]
            nombre_cliente = str(cliente)
            nombre_archivo = f"Vencimientos {nombre_cliente}.csv"
            client_id = df_cliente['client_id'].unique()
            
            df_cliente_a_mostrar = df_cliente[['Destino', 'NumeroFactura', 'FechaFactura', 
                                      'Vencimiento', 'total', 'Saldo', 
                                      'ReferenciaBancaria', 'dias_credito', 'dias_vencido']]

            archivo_salida_sinergia = CrearDoumento(df_cliente_a_mostrar, PATH_FILES_SINERGIA)
            archivo_salida_sinergia.crear_documentos_csv(nombre_archivo)

            # Me falta agregar una funcion que alimente las lista de insert a la cardsystem.archivosgenerales.
            query = "INSERT INTO [NexusFuel].[CardSystem].[ArchivosGenerales]"
            
            with engine.connect() as conn:

                conn.execute()

            
        else:
            CLIENTES_NO_ENCONTRADOS.append(cliente)





    print(("-> Fin de la ejecucion del programa. <-".upper()))