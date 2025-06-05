import time
import pandas as pd
import os
import json
import shutil
from functools import wraps
from mysql.connector import connect, errorcode, Error
from dotenv import load_dotenv

# Configuración inicial
load_dotenv()
config = {
    "user": os.environ['DATABASE_USERNAME'],
    "password": os.environ['DATABASE_PASSWORD'],
    "host": os.environ['DATABASE_HOST'],
    "database": os.environ['DATABASE_NAME'],
    "charset": 'utf8'
}

# Decorador para medir tiempo
def timeit(func):
    @wraps(func)
    def timeit_wrapper(*args, **kwargs):
        start_time = time.perf_counter()
        result = func(*args, **kwargs)
        end_time = time.perf_counter()
        total_time = end_time - start_time
        return (result, total_time)
    return timeit_wrapper

# Algoritmos de ordenamiento
@timeit
def bubble_sort(df, column='FECHA_VENTA'):
    df = df.copy()
    n = len(df)
    for i in range(n):
        swapped = False
        for j in range(0, n-i-1):
            if df.iloc[j][column] > df.iloc[j+1][column]:
                df.iloc[j], df.iloc[j+1] = df.iloc[j+1], df.iloc[j].copy()
                swapped = True
        if not swapped:
            break
    return df

@timeit
def quick_sort(df, column='FECHA_VENTA'):
    return df.sort_values(column, kind='quicksort')

@timeit
def merge_sort(df, column='FECHA_VENTA'):
    return df.sort_values(column, kind='mergesort')

@timeit
def heap_sort(df, column='FECHA_VENTA'):
    return df.sort_values(column, kind='heapsort')

def export_data(all_sorted_dfs, format_type, sort_method, export_dir, iteration_times):
    format_type = format_type.upper()
    base_name = f"ventas_{sort_method}"
    extension = format_type.lower() if format_type != 'TXT' else 'txt'
    file_name = f"{base_name}.{extension}"
    file_path = os.path.join(export_dir, file_name)
    
    # Crear directorio de resultados detallados
    detailed_dir = os.path.join(export_dir, 'detailed_results')
    os.makedirs(detailed_dir, exist_ok=True)
    
    # Guardar tiempos de iteración
    times_file = os.path.join(detailed_dir, f"times_{sort_method}.json")
    with open(times_file, 'w') as f:
        json.dump({
            'sort_method': sort_method,
            'iteration_times': iteration_times,
            'average_time': sum(iteration_times)/len(iteration_times),
            'min_time': min(iteration_times),
            'max_time': max(iteration_times)
        }, f, indent=2)
    
    start_time = time.time()
    
    try:
        if format_type == 'CSV':
            pd.concat(all_sorted_dfs).to_csv(file_path, index=False)
        elif format_type == 'JSON':
            # Para JSON, guardamos solo el último dataframe ordenado para evitar archivos muy grandes
            all_sorted_dfs[-1].to_json(file_path, orient='records', indent=2)
        elif format_type == 'PARQUET':
            pd.concat(all_sorted_dfs).to_parquet(file_path, engine='pyarrow')
        elif format_type == 'TXT':
            pd.concat(all_sorted_dfs).to_csv(file_path, index=False, sep='\t')
        else:
            raise ValueError(f"Formato no soportado: {format_type}")
        
        file_size = os.path.getsize(file_path)
        return file_path, file_size, time.time() - start_time
    
    except Exception as e:
        print(f"Error exportando {format_type}: {str(e)}")
        return None, None, None

def main():
    # Configuración inicial
    export_dir = 'exports'
    os.makedirs(export_dir, exist_ok=True)
    
    # Limpiar directorio de exportaciones anteriores
    for f in os.listdir(export_dir):
        file_path = os.path.join(export_dir, f)
        try:
            if os.path.isdir(file_path):
                shutil.rmtree(file_path)
            else:
                os.unlink(file_path)
        except Exception as e:
            print(f"Error al eliminar {file_path}: {e}")
    
    # Conexión a la base de datos
    cnx = get_connection()
    if not cnx:
        print("Error de conexión a la base de datos")
        return
    
    try:
        print("Connection established")
        # Cambié la consulta para obtener más datos significativos
        data = get_data(cnx, "SELECT * FROM VENTAS LIMIT 100")  # Asegúrate que el nombre de la tabla es correcto
    except Error as e:
        print(f"Error al obtener datos: {e}")
        return
    finally:
        cnx.close()
    
    if not data:
        print("No se obtuvieron datos de la base de datos")
        return
    
    # Verificar que la columna FECHA_VENTA existe
    df = pd.DataFrame(data, columns=['ID_VENTA', 'FECHA_VENTA', 'ID_CLIENTE', 'ID_EMPLEADO',
                   'ID_PRODUCTO', 'CANTIDAD', 'PRECIO_UNITARIO', 'DESCUENTO', 'FORMA_PAGO'])
    
    # Verificar que la columna de ordenación existe
    if 'FECHA_VENTA' not in df.columns:
        print("Error: La columna FECHA_VENTA no existe en el DataFrame")
        print("Columnas disponibles:", df.columns.tolist())
        return
    
    sort_methods = {
        'bubble': bubble_sort,
        'quick': quick_sort,
        'merge': merge_sort,
        'heap': heap_sort
    }
    
    formats = ['CSV', 'JSON', 'PARQUET', 'TXT']
    results = []
    
    for method_name, method_func in sort_methods.items():
        print(f"\nProbando {method_name} sort...")
        sort_times = []
        all_sorted = []
        
        for i in range(1, 51):
            sorted_df, sort_time = method_func(df)
            sort_times.append(sort_time)
            all_sorted.append(sorted_df)
            
            if i % 10 == 0:
                print(f"Iteración {i}: {sort_time:.5f}s")
        
        stats = {
            'method': method_name,
            'avg_sort': sum(sort_times)/len(sort_times),
            'min_sort': min(sort_times),
            'max_sort': max(sort_times)
        }
        
        for fmt in formats:
            path, size, export_time = export_data(all_sorted, fmt, method_name, export_dir, sort_times)
            if path:
                results.append({
                    **stats,
                    'format': fmt,
                    'export_time': export_time,
                    'file_size': size,
                    'file_path': path
                })
                print(f"Archivo generado: {path}")
            else:
                print(f"Error al generar archivo para {method_name} en formato {fmt}")
    
    # Guardar resultados
    if results:
        results_df = pd.DataFrame(results)
        results_path = os.path.join(export_dir, 'resultados_completos.csv')
        results_df.to_csv(results_path, index=False)
        print(f"\nResultados guardados en: {results_path}")
    else:
        print("\nNo se generaron resultados para guardar")
    
    print("\nResumen de resultados:")
    if results:
        print(results_df.groupby(['method', 'format']).agg({
            'avg_sort': 'mean',
            'export_time': 'mean',
            'file_size': 'mean'
        }).to_string())
    
    print(f"\nArchivos guardados en: {os.path.abspath(export_dir)}")

def get_connection():
    try:
        return connect(**config)
    except Error as err:
        print(f"Error de conexión: {err}")
        return None

def get_data(connection, query):
    cursor = connection.cursor()
    try:
        cursor.execute(query)
        return cursor.fetchall()
    except Error as e:
        print(f"Error al ejecutar consulta: {e}")
        return None
    finally:
        cursor.close()

if __name__ == "__main__":
    main()