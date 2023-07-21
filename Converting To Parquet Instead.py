# -*- coding: utf-8 -*-
"""
Created on Sun Jul 9 15:06:54 2023

@author: Dan
"""
#pip install pandas
import pandas as pd
#pip install  pyarrow
import pyarrow as pa
import pyarrow.parquet
import os
import time

#time starting
start_time = time.time()


chunk_size =1000000

#Read Csv file
new_york_calls = pd.read_csv('C://Users/Dan/Desktop/Python Files/NY Calls/311_Service_Requests_from_2010_to_Present.csv',chunksize=chunk_size)

###################################################################

#chunk counter starts at zero
idx = 0

chunk_size = 1000000


for chunk in pd.read_csv('C://Users/Dan/Desktop/Python Files/NY Calls/311_Service_Requests_from_2010_to_Present.csv', chunksize=chunk_size):
    #turning each value into a string
    chunk = chunk.astype(str)
    #converting each chunk into a parquet file (gzip) in said file location
    chunk.to_parquet(f'C://Users/Dan/Desktop/Python Files/NY Calls/all_parquet_files/chunk{idx}.parquet.gzip', compression='gzip')
    
    #add chunk counter
    idx += 1
    print(f'Chunk Number {idx}')

    
####################################################################
#Turning Parquet File into a Data Frame (from locally saved file)
#chunk counter(per file)
import pandas as pd
import os
import pyarrow
file_par= 0
#blank DF
ny_calls = pd.DataFrame()

#locating folder with all of our parquet files
for file in os.listdir("C:/Users/Dan/Desktop/Python Files/NY Calls/all_parquet_files"):
    if file.endswith(".parquet.gzip"):
        
        #making a file path that combines each idividual file name with folder location(namesake wise)
        filepath = os.path.join("C:/Users/Dan/Desktop/Python Files/NY Calls/all_parquet_files", file)
        
        #reading curent parquet file
        df = pd.read_parquet(filepath, engine='pyarrow')
        
        #any errors that interfer with dtypes (line 58) with be turned into NaN
        df[['Unique Key','Incident Zip','X Coordinate (State Plane)','Y Coordinate (State Plane)']] = df[['Unique Key','Incident Zip','X Coordinate (State Plane)','Y Coordinate (State Plane)']].apply(pd.to_numeric, errors='coerce') 
        
        #using dtypes to decrease file size per column
        df =df.astype({'Unique Key':'int32','Incident Zip':'float32','X Coordinate (State Plane)':'float32','Y Coordinate (State Plane)':'float32'})
        
        #add  new data frame to blank dataframe(line 48)
        ny_calls = pd.concat([ny_calls,df])
        
        #file chunk counter
        file_par +=1
        print(f'{file_par} files have been process/converted')

#####################################################################


####Time End
end_time = time.time()

#Calculate time
time_taken = end_time - start_time

print(f'This task took {time_taken} seconds!')

#This task took 311.4292800426483 seconds!
#Yes! Finally it works! and the file is so small.


