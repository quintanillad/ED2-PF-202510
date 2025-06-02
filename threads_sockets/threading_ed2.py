import random
import threading
import time


class Working_Thread(threading.Thread):
    def __init__(self, name):
        threading.Thread.__init__(self)
        self.name = name
        self.id = id(self)

    def run(self):
        """
        Run the thread
        """
        worker(self.name, self.id)


def worker(name: str, instance_id: int) -> None:
    print(f'Started worker {name} - {instance_id}')
    worker_time = random.choice(range(1, 10))
    time.sleep(worker_time)
    print("the result is", instance_id*2)
    print(f'{name} - {instance_id} worker finished in '
          f'{worker_time} seconds')


if __name__ == '__main__':
    for i in range(1000):
        thread = Working_Thread(name=f'computer_{i}')
        thread.start()
    thread.join()

def bubble_sort(data):
    # Implementación de bubblesort
    return sorted_data, time_taken

def quick_sort(data):
    # Implementación de quicksort
    return sorted_data, time_taken

# Modificar WorkerThread para usar algoritmos
class SortingThread(threading.Thread):
    def __init__(self, name, algorithm, data):
        threading.Thread.__init__(self)
        self.name = name
        self.algorithm = algorithm
        self.data = data
        self.result = None
        self.time = None
    
    def run(self):
        start = time.time()
        self.result = self.algorithm(self.data.copy())
        self.time = time.time() - start