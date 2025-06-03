import pandas as pd
import time
import os
import json
import fastavro
import pyarrow.parquet as pq
from datetime import datetime

def convert_date_fields(record):
    """Convierte campos datetime.date a strings ISO format"""
    for key, value in record.items():
        if hasattr(value, 'isoformat'):  # Maneja date y datetime
            record[key] = value.isoformat()
    return record

def export_to_csv(df, filename):
    """Exportar DataFrame a CSV"""
    start = time.time()
    df.to_csv(filename, index=False)
    size = os.path.getsize(filename)
    return time.time() - start, size

def export_to_json(df, filename):
    """Exportar DataFrame a JSON"""
    start = time.time()
    df.to_json(filename, orient='records', date_format='iso')
    size = os.path.getsize(filename)
    return time.time() - start, size

def export_to_parquet(df, filename):
    """Exportar DataFrame a Parquet"""
    start = time.time()
    df.to_parquet(filename, engine='pyarrow')
    size = os.path.getsize(filename)
    return time.time() - start, size

def export_to_avro(df, filename):
    """Exportar DataFrame a Avro con manejo de fechas"""
    start = time.time()
    
    # Definir schema Avro con tipos correctos
    schema = {
        'type': 'record',
        'name': 'Venta',
        'fields': [
            {'name': 'ID_VENTA', 'type': 'string'},
            {'name': 'FECHA_VENTA', 'type': 'string'},  # Usar string para fechas
            {'name': 'ID_CLIENTE', 'type': 'int'},
            {'name': 'ID_EMPLEADO', 'type': 'int'},
            {'name': 'ID_PRODUCTO', 'type': 'int'},
            {'name': 'CANTIDAD', 'type': 'int'},
            {'name': 'PRECIO_UNITARIO', 'type': 'double'},
            {'name': 'DESCUENTO', 'type': 'double'},
            {'name': 'FORMA_PAGO', 'type': 'string'}
        ]
    }
    
    # Convertir DataFrame a registros Avro y formatear fechas
    records = [convert_date_fields(record) for record in df.to_dict('records')]
    
    # Escribir archivo Avro
    with open(filename, 'wb') as out:
        fastavro.writer(out, schema, records)
    
    size = os.path.getsize(filename)
    return time.time() - start, size

def compare_export_methods(df, base_filename='ventas'):
    """Comparar los 4 métodos de exportación con manejo de errores"""
    results = []
    
    # CSV
    try:
        time_taken, size = export_to_csv(df, f"{base_filename}.csv")
        results.append({'format': 'CSV', 'time': time_taken, 'size': size})
    except Exception as e:
        print(f"❌ Error exportando a CSV: {str(e)}")
        results.append({'format': 'CSV', 'error': str(e)})
    
    # JSON
    try:
        time_taken, size = export_to_json(df, f"{base_filename}.json")
        results.append({'format': 'JSON', 'time': time_taken, 'size': size})
    except Exception as e:
        print(f"❌ Error exportando a JSON: {str(e)}")
        results.append({'format': 'JSON', 'error': str(e)})
    
    # Parquet
    try:
        time_taken, size = export_to_parquet(df, f"{base_filename}.parquet")
        results.append({'format': 'Parquet', 'time': time_taken, 'size': size})
    except Exception as e:
        print(f"❌ Error exportando a Parquet: {str(e)}")
        results.append({'format': 'Parquet', 'error': str(e)})
    
    # Avro
    try:
        time_taken, size = export_to_avro(df, f"{base_filename}.avro")
        results.append({'format': 'Avro', 'time': time_taken, 'size': size})
    except Exception as e:
        print(f"❌ Error exportando a Avro: {str(e)}")
        results.append({'format': 'Avro', 'error': str(e)})
    
    return pd.DataFrame(results)