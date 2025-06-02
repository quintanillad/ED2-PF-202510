import socket
import threading
import json
import pandas as pd
from threading_ed2 import bubble_sort, quick_sort, merge_sort, heap_sort

class SortingServerThread(threading.Thread):
    def __init__(self, client_socket, client_address):
        threading.Thread.__init__(self)
        self.client_socket = client_socket
        self.client_address = client_address
    
    def run(self):
        try:
            data = self.client_socket.recv(16384)
            if not data:
                return
                
            request = json.loads(data.decode())
            df = pd.DataFrame(request['data'])
            algorithm = request['algorithm']
            
            if algorithm == 'bubble':
                result, time_taken = bubble_sort(df)
            elif algorithm == 'quick':
                result, time_taken = quick_sort(df)
            elif algorithm == 'merge':
                result, time_taken = merge_sort(df)
            elif algorithm == 'heap':
                result, time_taken = heap_sort(df)
            else:
                raise ValueError("Algoritmo no soportado")
            
            response = {
                'algorithm': algorithm,
                'time': time_taken,
                'sorted_data': result.to_dict('records')
            }
            
            self.client_socket.sendall(json.dumps(response).encode())
            
        except Exception as e:
            print(f"Error con cliente {self.client_address}: {str(e)}")
        finally:
            self.client_socket.close()

def start_server(host='localhost', port=8080):
    server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    server_socket.bind((host, port))
    server_socket.listen(5)
    print(f"Servidor escuchando en {host}:{port}")
    
    try:
        while True:
            client_socket, client_address = server_socket.accept()
            print(f"Conexión aceptada de {client_address}")
            thread = SortingServerThread(client_socket, client_address)
            thread.start()
    finally:
        server_socket.close()

if __name__ == "__main__":
    start_server()