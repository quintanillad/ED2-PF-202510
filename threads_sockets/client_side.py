import socket
import json
import pandas as pd
from sorting_algorithms import SortingThread

SERVER = "192.168.1.8"
PORT = 8080

def main():
    # Cargar datos (ejemplo)
    df = pd.read_csv('ventas.csv')
    
    # Crear hilos para cada algoritmo
    algorithms = ['bubble', 'quick', 'merge', 'heap']
    threads = []
    results = {}
    
    for algo in algorithms:
        client_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        client_socket.connect((SERVER, PORT))
        
        # Preparar datos para enviar
        request = {
            'algorithm': algo,
            'data': df.to_dict('records')
        }
        
        # Enviar solicitud
        client_socket.sendall(bytes(json.dumps(request), 'UTF-8'))
        
        # Recibir respuesta
        response_data = client_socket.recv(16384)
        response = json.loads(response_data.decode())
        
        if 'error' not in response:
            results[algo] = {
                'time': response['time'],
                'data': pd.DataFrame(response['sorted_data'])
            }
            print(f"{algo} sort completado en {response['time']:.4f} segundos")
            
            # Guardar resultados
            pd.DataFrame(response['sorted_data']).to_csv(f'sorted_{algo}.csv', index=False)
        else:
            print(f"Error en {algo} sort: {response['error']}")
            
        client_socket.close()
    
    # Análisis comparativo
    if results:
        print("\nResumen de tiempos:")
        for algo, res in results.items():
            print(f"{algo}: {res['time']:.4f} segundos")
        
        # agregar el análisis ANOVA
        # ...

if __name__ == "__main__":
    main()