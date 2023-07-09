# -*- coding: utf-8 -*-
"""
Created on Sat Jul  8 02:07:41 2023

@author: Dan
"""
import pandas as pd
import numpy as np
import time

#time starting
start_time = time.time()





chunk_size=1000000
ny_calls_clean = pd.read_csv('C://Users/Dan/Desktop/Python Files/NY Calls/Modified NY Calls.csv',usecols=['Agency Name','Complaint Type','City','Created Date'],chunksize=chunk_size)


###############   creating blank data frame   ####################
New_York_calls = pd.DataFrame()
#chunk counter
idx = 0

#looping to batch values in and add to chunk counter per chunk loaded in
for idx, chunk in enumerate (ny_calls_clean):
    
    idx += 1
    New_York_calls = pd.concat([New_York_calls,chunk])
    #chunk['Created Data'] = pd.to_datetime(chunk['Created Date']).dt.year
    
    print(chunk)
    print(idx)
    
    if idx == 34:
        break

print('file read')


########   converting (Created Date Column) time in simplfied year      ##############
New_York_calls['Created Date'] = pd.to_datetime(New_York_calls['Created Date']).dt.year
#New_York_calls['Created Date'] = New_York_calls['Created Date'].astype(np.int16)

print('hey data is done loading')


#Creating New Data Frames With Altered Values
new_york_popular_agency = pd.DataFrame()

new_york_cities_data = pd.DataFrame()

new_york_complaints_data = pd.DataFrame()

new_york_years_data = pd.DataFrame()

###################Playing around with the data #############
popular_agency = New_York_calls['Agency Name'].value_counts()

popular_cities = New_York_calls['City'].value_counts()

popular_complaints = New_York_calls['Complaint Type'].value_counts()

calls_per_year = New_York_calls['Created Date'].value_counts()
##############################################################

#Final DF for Agency Data
new_york_popular_agency = pd.concat([new_york_popular_agency,popular_agency])
#turing index into column
new_york_popular_agency.reset_index(inplace=True)



#Final DF for City Data
new_york_cities_data = pd.concat([new_york_cities_data,popular_cities])
#turing index into column
new_york_cities_data.reset_index(inplace=True)



#Final DF for Complaint Data
new_york_complaints_data = pd.concat([new_york_complaints_data,popular_complaints])
#turing index into column
new_york_complaints_data.reset_index(inplace=True)



#Final DF for Years Data
new_york_years_data = pd.concat([new_york_years_data,calls_per_year],axis=1)
#turing index into column
new_york_years_data.reset_index(inplace=True)

################# Creating CSV Files For Each Data Frame ###########

new_york_popular_agency.to_csv('NY Agency Calls Data.csv')

new_york_cities_data.to_csv('NY City Calls Data.csv')

new_york_complaints_data.to_csv('NY Complaint Calls Data.csv')

new_york_years_data.to_csv('NY Call Yearly Data.csv')


####Time End
end_time = time.time()

#Calculate time
time_taken = end_time - start_time

print(f'This task took {time_taken} seconds!')










