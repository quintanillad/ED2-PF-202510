from mysql.connector import connect, errorcode, Error
import os
from os import environ
from dotenv import load_dotenv 
import pandas as pd
import time


load_dotenv()

config = {
    "user": environ['DATABASE_USERNAME'],
    "password": environ['DATABASE_PASSWORD'],
    "host": environ['DATABASE_HOST'],
    "database": environ['DATABASE_NAME'],
    "charset": 'utf8'
}


def get_connection():
    try:
        return connect(**config)
    except Error as err:
        if err.errno == errorcode.ER_ACCESS_DENIED_ERROR:
            print("Something is wrong with your user name or password")
        elif err.errno == errorcode.ER_BAD_DB_ERROR:
            print("Database does not exist")
        else:
            print(err)
        return None


def get_data(connection: connect, query: str):
    my_cursor = connection.cursor()
    my_cursor.execute(query)
    data = my_cursor.fetchall()
    my_cursor.close()
    return data


cnx = get_connection()

print("Connection established")

data = get_data(cnx, "SELECT * FROM UN.VENTAS LIMIT 10")


df = pd.DataFrame(data, columns=['ID_VENTA', 'FECHA_VENTA', 'ID_CLIENTE', 'ID_EMPLEADO',
                  'ID_PRODUCTO', 'CANTIDAD', 'PRECIO_UNITARIO', 'DESCUENTO', 'FORMA_PAGO'])

print(df)

def export_data(df, format_type):
    format_type = format_type.upper()
    file_name = f"ventas.{format_type.lower() if format_type != 'TXT' else 'txt'}"
    start_time = time.time()
    
    if format_type == 'CSV':
        df.to_csv(file_name, index=False)
    elif format_type == 'JSON':
        df.to_json(file_name, orient='records')
    elif format_type == 'PARQUET':
        df.to_parquet(file_name, engine='pyarrow')
    elif format_type == 'TXT':
        df.to_csv(file_name, index=False, sep='\t')
    else:
        raise ValueError(f"Formato no soportado: {format_type}")
    
    file_size = os.path.getsize(file_name)
    export_time = time.time() - start_time
    
    return file_name, file_size, export_time

formats = ['CSV', 'JSON','PARQUET','TXT']
for fmt in formats:
    name, size, export_duration = export_data(df, fmt)
    print(f"Exportado {name} - Tamaño: {size} bytes - Tiempo: {export_duration:.4f}s")