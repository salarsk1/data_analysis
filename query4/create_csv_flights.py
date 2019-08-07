import pandas as pd
'''
Download 12 months from January 2018 to December 2018 from 

https://www.transtats.bts.gov/Tables.asp?DB_ID=120&DB_Name=Airline%20On-Time%20Performance%20Data&DB_Short_Name=On-Time

and clicking "Download" under the "Reporting Carrier On-TimePerformance(1987-present)" link. 
You have to download each month individually then merge them into a single
CSV, or add them to your db monthly. Select the following categories:
 
"YEAR","MONTH", "DAY_OF_MONTH", "DAY_OF_WEEK", "OP_UNIQUE_CARRIER", "TAIL_NUM", "OP_CARRIER_FL_NUM",
"ORIGIN_AIRPORT_ID", "DEST_AIRPORT_ID", "CRS_DEP_TIME", "DEP_TIME", "DEP_DELAY ", "CRS_ARR_TIME",
"ARR_TIME", "ARR_DELAY", "CANCELLED", "CANCELLATION_CODE", "CRS_ELAPSED_TIME","ACTUAL_ELAPSED_TIME", 
"AIR_TIME", "DISTANCE", "CARRIER_DELAY", "WEATHER_DELAY", "NAS_DELAY", "SECURITY_DELAY", "LATE_AIRCRAFT_DELAY"

'''

# After getting the files append them to make the flights table for the entire year
df = pd.DataFrame()
for i in range(1,13):
    filename = "On_Time_Reporting_Carrier_On_Time_Performance_(1987_present)_2018_"
    filename += str(i)+'.csv'
    df_temp = pd.read_csv(filename)
    df = df.append(df_temp, ignore_index = True)

# Rename some columns
df = df.rename(columns={'Year':'YEAR','Month':'MONTH','DayofMonth':'DAY_OF_MONTH', 
                        'DayOfWeek':'DAY_OF_WEEK',
                        'IATA_CODE_Reporting_Airline':'OP_UNIQUE_CARRIER',
                        'Tail_Number':'TAIL_NUM','Flight_Number_Reporting_Airline':'OP_CARRIER_FL_NUM',
                        'OriginAirportID':'ORIGIN_AIRPORT_ID', 'DestAirportID':'DEST_AIRPORT_ID',
                        'CRSDepTime':'CRS_DEP_TIME', 'DepTime':'DEP_TIME', 'DepDelay':'DEP_DELAY',
                        'CRSArrTime':'CRS_ARR_TIME', 'ArrTime':'ARR_TIME', 'ArrDelay':'ARR_DELAY',
                        'Cancelled':'CANCELLED', 'CancellationCode':'CANCELLATION_CODE',
                        'CRSElapsedTime':'CRS_ELAPSED_TIME', 'ActualElapsedTime':'ACTUAL_ELAPSED_TIME',
                        'AirTime':'AIR_TIME', 'Distance':'DISTANCE', 'CarrierDelay':'CARRIER_DELAY',
                        'WeatherDelay':'WEATHER_DELAY', 'NASDelay':'NAS_DELAY', 'SecurityDelay':'SECURITY_DELAY',
                        'LateAircraftDelay':'LATE_AIRCRAFT_DELAY'})

# Keep only the following columns from the data
new_columns = ['YEAR', 'MONTH', 'DAY_OF_MONTH', 'DAY_OF_WEEK',
               'OP_UNIQUE_CARRIER', 'TAIL_NUM','OP_CARRIER_FL_NUM',
               'ORIGIN_AIRPORT_ID', 'DEST_AIRPORT_ID',
               'CRS_DEP_TIME', 'DEP_TIME', 'DEP_DELAY',
               'CRS_ARR_TIME', 'ARR_TIME', 'ARR_DELAY',
               'CANCELLED', 'CANCELLATION_CODE',
               'CRS_ELAPSED_TIME', 'ACTUAL_ELAPSED_TIME',
               'AIR_TIME', 'DISTANCE', 'CARRIER_DELAY',
               'WEATHER_DELAY', 'NAS_DELAY', 'SECURITY_DELAY',
               'LATE_AIRCRAFT_DELAY']
df1 = df[new_columns]

# Write it to a csv file
df1.to_csv('/Users/salarsk/developments/courses/de2/hw5/flights.csv',index=False)