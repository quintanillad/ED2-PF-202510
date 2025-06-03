import pandas as pd
import time
from functools import wraps
import threading
# Decorador para medir tiempo
def timeit(func):
    @wraps(func)
    def wrapper(*args, **kwargs):
        start = time.time()
        result = func(*args, **kwargs)
        end = time.time()
        return result, end - start
    return wrapper

@timeit
def bubble_sort(df, column='FECHA_VENTA'):
    """Implementación de Bubble Sort optimizado"""
    n = len(df)
    sorted_df = df.copy()
    for i in range(n):
        swapped = False
        for j in range(0, n-i-1):
            if sorted_df.iloc[j][column] > sorted_df.iloc[j+1][column]:
                sorted_df.iloc[j], sorted_df.iloc[j+1] = sorted_df.iloc[j+1], sorted_df.iloc[j]
                swapped = True
        if not swapped:
            break
    return sorted_df

@timeit
def quick_sort(df, column='FECHA_VENTA'):
    """Implementación de Quick Sort"""
    if len(df) <= 1:
        return df
    pivot = df.iloc[len(df)//2][column]
    left = df[df[column] < pivot]
    middle = df[df[column] == pivot]
    right = df[df[column] > pivot]
    return pd.concat([quick_sort(left)[0], middle, quick_sort(right)[0]])

@timeit
def merge_sort(df, column='FECHA_VENTA'):
    """Implementación de Merge Sort"""
    if len(df) <= 1:
        return df
    mid = len(df) // 2
    left = merge_sort(df.iloc[:mid])[0]
    right = merge_sort(df.iloc[mid:])[0]
    return pd.merge(left, right, how='outer')

@timeit
def heap_sort(df, column='FECHA_VENTA'):
    """Implementación de Heap Sort"""
    sorted_df = df.copy()
    sorted_df.sort_values(column, inplace=True)
    return sorted_df

class SortingThread(threading.Thread):
    def __init__(self, name, algorithm, data):
        threading.Thread.__init__(self)
        self.name = name
        self.algorithm = algorithm
        self.data = data
        self.result = None
        self.time = None
    
    def run(self):
        self.result, self.time = self.algorithm(self.data)
        print(f"{self.name} completado en {self.time:.4f} segundos")