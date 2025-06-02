import socket
import threading
import json
from sorting_algorithms import bubble_sort, quick_sort, merge_sort, heap_sort
import pandas as pd

class ClientThread(threading.Thread):
    def __init__(self, clientAddress, clientsocket):
        threading.Thread.__init__(self)
        self.csocket = clientsocket
        print("New connection added: ", clientAddress)
        
    def run(self):
        print("Connection from: ", clientAddress)
        while True:
            try:
                # Recibir datos del cliente
                data = self.csocket.recv(16384)  # Buffer más grande para datos
                if not data:
                    break
                    
                # Decodificar y procesar
                msg = data.decode()
                if msg == 'bye':
                    break
                    
                # Cargar datos y tipo de algoritmo
                request = json.loads(msg)
                df_data = request['data']
                algorithm = request['algorithm']
                
                # Convertir a DataFrame
                df = pd.DataFrame(df_data)
                
                # Ejecutar algoritmo correspondiente
                if algorithm == 'bubble':
                    result, time = bubble_sort(df)
                elif algorithm == 'quick':
                    result, time = quick_sort(df)
                elif algorithm == 'merge':
                    result, time = merge_sort(df)
                elif algorithm == 'heap':
                    result, time = heap_sort(df)
                else:
                    raise ValueError("Algoritmo no soportado")
                
                # Preparar respuesta
                response = {
                    'algorithm': algorithm,
                    'time': time,
                    'sorted_data': result.to_dict('records')
                }
                
                # Enviar respuesta
                self.csocket.sendall(bytes(json.dumps(response), 'UTF-8'))
                
            except Exception as e:
                print(f"Error: {e}")
                self.csocket.sendall(bytes(json.dumps({'error': str(e)}), 'UTF-8'))
                break
                
        print("Client at ", clientAddress, " disconnected")

# Resto del código del servidor permanece igual...
LOCALHOST = "192.168.1.4"
PORT = 8080
server = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
server.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
server.bind((LOCALHOST, PORT))
print("Server started")
print("Waiting for client request..")
while True:
    server.listen(3)
    clientsock, clientAddress = server.accept()
    newthread = ClientThread(clientAddress, clientsock)
    newthread.start()
