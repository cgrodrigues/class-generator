from .data_generator import DataGenerator
import numpy as np
from datetime import datetime, timedelta



class DataGeneratorSS7(DataGenerator):
    
    def transaction_latency_ms(self, record):
        return   (record['endtime'] - record['starttime']).total_seconds() * 1000
    
    
    def msgtype(self, record):
        return np.random.choice([6,2,4,5,12,16], p=[0.58, 0.21, 0.099, 0.053,0.038, 0.020])
            
        
    def starttime(self, record): 
        return record['date']
        
        
    def endtime(self, record):
        duration = np.round(np.random.normal(record['mean_duration'], record['std_duration'])) 
        return record['starttime'] + timedelta(seconds=duration)
    
    def file(self, record):
        start_of_day = datetime(record['date'].year, record['date'].month, record['date'].day)
        time_diff = record['date'] - start_of_day
        period_number = int(time_diff.total_seconds() // 300)  # 300 seconds = 5 minutes

        return f"{record['date'].year}.{record['date'].month:02d}.{record['date'].day:02d}.{period_number}"
    
    





