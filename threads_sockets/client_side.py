import socket
import json
import pandas as pd
from threading_ed2 import SortingThread
from sql_connection import get_connection, get_data

def main():
    # Obtener datos de la base de datos
    cnx = get_connection()
    data = get_data(cnx, "SELECT * FROM UN.VENTAS LIMIT 100")  # Más datos para pruebas
    df = pd.DataFrame(data, columns=['ID_VENTA', 'FECHA_VENTA', 'ID_CLIENTE', 'ID_EMPLEADO',
                      'ID_PRODUCTO', 'CANTIDAD', 'PRECIO_UNITARIO', 'DESCUENTO', 'FORMA_PAGO'])
    cnx.close()
    
    # Algoritmos a probar
    algorithms = {
        'bubble': 'Bubble Sort',
        'quick': 'Quick Sort', 
        'merge': 'Merge Sort',
        'heap': 'Heap Sort'
    }
    
    results = {}
    
    for algo_key, algo_name in algorithms.items():
        try:
            # Configurar socket
            sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            sock.connect(('localhost', 8080))
            
            # Preparar y enviar datos
            request = {
                'algorithm': algo_key,
                'data': df.to_dict('records')
            }
            sock.sendall(json.dumps(request).encode())
            
            # Recibir respuesta
            response = json.loads(sock.recv(16384).decode())
            results[algo_key] = {
                'time': response['time'],
                'data': pd.DataFrame(response['sorted_data'])
            }
            
            print(f"{algo_name} completado en {response['time']:.4f} segundos")
            
            # Guardar resultados
            results[algo_key]['data'].to_csv(f'sorted_{algo_key}.csv', index=False)
            
        except Exception as e:
            print(f"Error con {algo_name}: {str(e)}")
        finally:
            sock.close()
    
    # Análisis comparativo
    if results:
        print("\nResumen de resultados:")
        for algo, res in results.items():
            print(f"{algorithms[algo]}: {res['time']:.4f} segundos")
        
        # Aquí iría el análisis ANOVA
        # ...

if __name__ == "__main__":
    main()