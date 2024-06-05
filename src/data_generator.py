import random
import numpy as np

class DataGenerator:
    def __init__(self):
        pass
    
    def generate_record(self, record={}, methods=None):
        
        if methods is None:
            return record
        
        for method in methods:
            record[method.__name__] = method(self, record)

            
        return record 
    
    def random_string(self, record):
        return ''.join(random.choice('abcdefghijklmnopqrstuvwxyz') for _ in range(10))

    def random_int(self, record):
        return random.randint(1, 100)

    def random_float(self, record):
        return np.random.rand()