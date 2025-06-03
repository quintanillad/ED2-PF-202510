import threading
import time
import pandas as pd
import numpy as np
from functools import wraps

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
    return df.sort_values(column, kind='bubble')

@timeit
def quick_sort(df, column='FECHA_VENTA'):
    return df.sort_values(column, kind='quicksort')

@timeit
def merge_sort(df, column='FECHA_VENTA'):
    return df.sort_values(column, kind='mergesort')

@timeit
def heap_sort(df, column='FECHA_VENTA'):
    return df.sort_values(column, kind='heapsort')

# Clase para ejecutar en hilos
class SortingThread(threading.Thread):
    def __init__(self, algorithm, df, column='FECHA_VENTA'):
        threading.Thread.__init__(self)
        self.algorithm = algorithm
        self.df = df.copy()
        self.column = column
        self.result = None
        self.time = None
        
    def run(self):
        sorted_df, exec_time = self.algorithm(self.df, self.column)
        self.result = sorted_df
        self.time = exec_time