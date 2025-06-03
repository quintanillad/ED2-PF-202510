import pandas as pd
import time
import os
import json
import fastavro
import pyarrow.parquet as pq

def export_to_csv(df, filename):
    """Exportar DataFrame a CSV"""
    start = time.time()
    df.to_csv(filename, index=False)
    size = os.path.getsize(filename)
    return time.time() - start, size

def export_to_json(df, filename):
    """Exportar DataFrame a JSON"""
    start = time.time()
    df.to_json(filename, orient='records')
    size = os.path.getsize(filename)
    return time.time() - start, size

def export_to_parquet(df, filename):
    """Exportar DataFrame a Parquet"""
    start = time.time()
    df.to_parquet(filename, engine='pyarrow')
    size = os.path.getsize(filename)
    return time.time() - start, size

def export_to_avro(df, filename):
    """Exportar DataFrame a Avro"""
    start = time.time()
    
    # Definir schema Avro
    schema = {
        'type': 'record',
        'name': 'Venta',
        'fields': [
            {'name': 'ID_VENTA', 'type': 'string'},
            {'name': 'FECHA_VENTA', 'type': 'string'},
            {'name': 'ID_CLIENTE', 'type': 'int'},
            {'name': 'ID_EMPLEADO', 'type': 'int'},
            {'name': 'ID_PRODUCTO', 'type': 'int'},
            {'name': 'CANTIDAD', 'type': 'int'},
            {'name': 'PRECIO_UNITARIO', 'type': 'double'},
            {'name': 'DESCUENTO', 'type': 'double'},
            {'name': 'FORMA_PAGO', 'type': 'string'}
        ]
    }
    
    # Convertir DataFrame a registros Avro
    records = df.to_dict('records')
    
    # Escribir archivo Avro
    with open(filename, 'wb') as out:
        fastavro.writer(out, schema, records)
    
    size = os.path.getsize(filename)
    return time.time() - start, size

def compare_export_methods(df, base_filename='ventas'):
    """Comparar los 4 métodos de exportación"""
    results = []
    
    # CSV
    time_taken, size = export_to_csv(df, f"{base_filename}.csv")
    results.append({'format': 'CSV', 'time': time_taken, 'size': size})
    
    # JSON
    time_taken, size = export_to_json(df, f"{base_filename}.json")
    results.append({'format': 'JSON', 'time': time_taken, 'size': size})
    
    # Parquet
    time_taken, size = export_to_parquet(df, f"{base_filename}.parquet")
    results.append({'format': 'Parquet', 'time': time_taken, 'size': size})
    
    # Avro
    time_taken, size = export_to_avro(df, f"{base_filename}.avro")
    results.append({'format': 'Avro', 'time': time_taken, 'size': size})
    
    return pd.DataFrame(results)