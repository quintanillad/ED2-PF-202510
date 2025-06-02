from mysql.connector import connect, errorcode, Error
from os import environ
import pandas as pd

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
    file_name = f"ventas.{format_type.lower()}"
    start_time = time.time()
    
    if format_type == 'CSV':
        df.to_csv(file_name, index=False)
    elif format_type == 'JSON':
        df.to_json(file_name, orient='records')
    # Añadir más formatos según necesidad
    
    file_size = os.path.getsize(file_name)
    export_time = time.time() - start_time
    
    return file_name, file_size, export_time

# Ejemplo de uso
formats = ['CSV', 'JSON']
for fmt in formats:
    name, size, time = export_data(df, fmt)
    print(f"Exportado {name} - Tamaño: {size} bytes - Tiempo: {time:.4f}s")