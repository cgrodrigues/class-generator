from src.data_generator_ss7 import DataGeneratorSS7
from datetime import datetime
import random
from faker import Faker
import numpy as np
import pandas as pd
import time
from datetime import datetime, timedelta



def generate_customer(customer_id, fake, n_customers: int = 5000):
    
    n_customers = max(round(n_customers*0.995), 1000-5)

    mean_duration = np.round(np.random.uniform(2,61416)) 
    std_duration = mean_duration/2 
    mean_nb_calls_per_day = np.round(np.random.uniform(1,40))
    
    local_network = '26803' # always the same local network (NOS PT = 268 03)
    partner_mcc = random.randint(100, 999)  # Mobile Country Code
    partner_mnc = random.randint(10, 99)    # Mobile Network Code
    msin = random.randint(1000000000, 9999999999)  # Mobile Subscription Identification Number
    partner_network =  f"{partner_mcc}{partner_mnc}"
    imsi = f"{partner_mcc}{partner_mnc}{msin}"


    return [customer_id, 
            imsi,
            mean_duration,
            std_duration,
            mean_nb_calls_per_day,
            local_network,
            partner_network,
            partner_mcc,
            partner_mnc]
    
def generate_customer_profiles_table(n_customers: int = 5000):
    Faker.seed(0) 
    fake = Faker()
    
    if n_customers < 1000:
        n_customers = 1000
    
    customer_properties=[]
    
    for customer_id in range(max(round(n_customers*0.995), 1000-5)):
        cust = generate_customer(customer_id, fake, n_customers=n_customers)
        customer_properties.append(cust)
        
       
    customer_profiles = pd.DataFrame(customer_properties, columns=['customer_id',
                                                                    'imsi',
                                                                    'mean_duration', 
                                                                    'std_duration',
                                                                    'mean_nb_calls_per_day',
                                                                    'local_network',
                                                                    'partner_network',
                                                                    'partner_mcc',
                                                                    'partner_mnc'])
    return customer_profiles


def generate_calls(customer_profile, start_datetime): 
       
    Faker.seed(0) 
    fake = Faker()
    customer_calls = []
    
       
    # Random number of calls for that day 
    nb_calls = int(np.random.poisson(customer_profile.mean_nb_calls_per_day))
    if nb_calls > 0:
        for call in range(nb_calls):
            # Time of call: Around noon, std 20000 seconds. This choice aims at simulating the fact that 
            # most calls occur during the day.
            time_call = int(np.random.normal(86400/2, 20000))
            
            
            # If call time between 0 and 86400, let us keep it, otherwise, let us discard it
            if (time_call>0) and (time_call<86400-1):
                # Event start date of the call
                curr_start_datetime = start_datetime + timedelta(seconds=time_call)
                
                
                dgss7 = DataGeneratorSS7()

                rec = dgss7.generate_record(record={'_date': curr_start_datetime, 
                                                    '_mean_duration': customer_profile.mean_duration,
                                                    '_std_duration': customer_profile.std_duration,
                                                    'date': curr_start_datetime.strftime('%Y-%m-%d %H:%M:%S'), 
                                                    'imsi': customer_profile.imsi, 
                                                    'Partner_MCC':customer_profile.partner_mcc,
                                                    'Partner_MNC':customer_profile.partner_mnc} ,
                                            methods=[DataGeneratorSS7.msgtype, 
                                                    DataGeneratorSS7.starttime, 
                                                    DataGeneratorSS7.endtime,
                                                    DataGeneratorSS7.transaction_latency_ms,
                                                    DataGeneratorSS7.file,
                                                    DataGeneratorSS7.numOfIncomingMSU])
                
        
                customer_calls.append(rec)
                
                    
                    
    customer_calls = pd.DataFrame(customer_calls)
    
    print(customer_calls)
    
    return customer_calls
    
def main(nb_days, start_date, path):

    
    start_date = datetime.strptime(start_date, "%Y-%m-%d")
    
    start_time=time.time()
    customer_table = generate_customer_profiles_table(n_customers = 1000)
    print("Time to generate customer profiles table: {0:.2}s".format(time.time()-start_time))
    
    start_datetime = start_date
    for day in range(nb_days):
        print(f"Day: {start_datetime}")
        
        start_time=time.time()
        
        start_datetime = start_date + timedelta(days=day)
        
        calls_df = customer_table.groupby('customer_id').apply(lambda x : generate_calls(x.iloc[0], start_datetime=start_datetime)).reset_index(drop=True)
       
        print("Time to generate calls recors: {0:.2}s".format(time.time()-start_time))
    
        calls_df=calls_df.sort_values('date')
 
        # calls_df.to_parquet(path, partition_cols=['date'], index=False)
        calls_df.to_parquet(path, partition_cols=['file'], index=False)
        
        print(f"Save parquet file: HOUR_KEY_DATE={start_datetime.strftime('%Y%m%d%H%M%S')}")


if __name__ == "__main__":
    main(nb_days =  1,
        start_date="2024-02-23",
        path ="./data/output")
    
