# -*- coding: utf-8 -*-
"""
Created on Mon Jul 10 15:06:54 2023

@author: Dan
"""
#pip install pandas
import pandas as pd
#pip install  pyarrow
import pyarrow as pa
import pyarrow.parquet as pq

import time

#time starting
start_time = time.time()


chunk_size =1000000

#Read Csv file
new_york_calls = pd.read_csv('C://Users/Dan/Desktop/Python Files/NY Calls/311_Service_Requests_from_2010_to_Present.csv',chunksize=chunk_size)

#read "vehicle column as a string"
schema = pa.schema([('Vehicle Type',pa.string())   
      ])              

                    
###################################################################
#Make lsit of chunks (34) to be compiled in to parquet file
parquet_chunks = []

#chunk counter starts at zero
idx = 0

for chunk in new_york_calls:
     
    #changing "vehicle" column as a string
    chunk['Vehicle Type'] = chunk['Vehicle Type'].astype(str)                 
    
    #Making pyarrow table from chunks and keeping schema in mind
    table = pa.Table.from_pandas(chunk, schema = schema)
    
    #adding tables to blank list
    parquet_chunks.append(table)

    #updates chunk counter with sentence    
    idx += 1
    print(f' Chunk Number {idx}')
    
    if idx == 34:
        break
####################################################################


#combining all the parquet tables  
parquet_table = pa.concat_tables(parquet_chunks)

#With all parquet tables. Making a parquet file
pq.write_table(parquet_table,'Ny Calls.parquet')

####Time End
end_time = time.time()

#Calculate time
time_taken = end_time - start_time

print(f'This task took {time_taken} seconds!')

#This task took 311.4292800426483 seconds!
#Yes! Finally it works! and the file is so small.
