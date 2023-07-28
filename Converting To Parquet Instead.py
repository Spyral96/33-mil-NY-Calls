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

print('starting parquet to data frame')

file_par= 0
#blank DF
ny_calls = pd.DataFrame()

#locating folder with all of our parquet files
for file in os.listdir("C:/Users/Dan/Desktop/Python Files/NY Calls/all_parquet_files"):
    if file.endswith(".parquet.gzip"):
        
        #making a file path that combines each idividual file name with folder location(namesake wise)
        filepath = os.path.join("C:/Users/Dan/Desktop/Python Files/NY Calls/all_parquet_files", file)
        
        #reading curent parquet file
        df = pd.read_parquet(filepath,columns=['Agency Name','Complaint Type','City','Created Date'])
        

        #add  new data frame to blank dataframe(line 48)
        ny_calls = pd.concat([ny_calls, df], ignore_index=True)

        
        ny_calls['Created Date'] = pd.to_datetime(ny_calls['Created Date']).dt.year

        #file chunk counter
        file_par +=1
        print(f'{file_par} files have been process/converted')



#####################################################################

########   converting (Created Date Column) time in simplfied year      ##############

# Creating new DataFrames with altered values
new_york_popular_agency = pd.DataFrame()
new_york_cities_data = pd.DataFrame()
new_york_complaints_data = pd.DataFrame()
new_york_years_data = pd.DataFrame()

###################Playing around with the data #############
popular_agency = ny_calls['Agency Name'].value_counts()
popular_cities = ny_calls['City'].value_counts()
popular_complaints = ny_calls['Complaint Type'].value_counts()
calls_per_year = ny_calls['Created Date'].value_counts()
##############################################################

# Final DF for Agency Data
new_york_popular_agency = pd.concat([new_york_popular_agency, popular_agency])
# Turning index into column
new_york_popular_agency.reset_index(inplace=True)

# Final DF for City Data
new_york_cities_data = pd.concat([new_york_cities_data, popular_cities])
# Turning index into column
new_york_cities_data.reset_index(inplace=True)

# Final DF for Complaint Data
new_york_complaints_data = pd.concat([new_york_complaints_data, popular_complaints])
# Turning index into column
new_york_complaints_data.reset_index(inplace=True)

# Final DF for Years Data
new_york_years_data = pd.concat([new_york_years_data, calls_per_year], axis=1)
# Turning index into column
new_york_years_data.reset_index(inplace=True)



print('converting to csv files')

#Converting to CSV file
ny_calls.to_csv('NY York Calls Graphing Data.csv')

transformed_data =pd.DataFrame()

transformed_data = pd.concat([transformed_data,new_york_popular_agency,new_york_cities_data,new_york_complaints_data,new_york_years_data])
transformed_data.to_csv('transformed_data.csv',index=False)
####Time End
end_time = time.time()

#Calculate time
time_taken = end_time - start_time

print(f'This task took {time_taken} seconds!')

#This task took This task took 5236.458662986755 seconds!
#WIP oh boy too long


