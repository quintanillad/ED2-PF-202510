import socket
import pickle
import pandas as pd
from sorting_algorithms import bubble_sort, quick_sort, merge_sort, heap_sort

class SocketManager:
    def __init__(self, host='localhost', port=8080):
        self.host = host
        self.port = port
        self.algorithms = {
            'bubble': bubble_sort,
            'quick': quick_sort,
            'merge': merge_sort,
            'heap': heap_sort
        }
    
    def send_data(self, df):
        """Envía datos al servidor y recibe resultados"""
        try:
            with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
                sock.settimeout(10)  # 10 segundos de timeout
                sock.connect((self.host, self.port))
                
                # Enviar datos
                sock.sendall(pickle.dumps(df))
                
                # Recibir resultados
                results = pickle.loads(sock.recv(65536))
                return results
                
        except Exception as e:
            print(f"Error de conexión: {str(e)}")
            return None