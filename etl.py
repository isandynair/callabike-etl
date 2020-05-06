import pandas as pd
from sqlalchemy import create_engine
import numpy as np
import datetime
from datetime import date

class ETL():

  def load_from_sql(self):

    csvdb = create_engine('sqlite:///csvdb.db')

    chunksize = 100000
    i=0
    j=0

    print("Converting booking csv to sqlite table..\n")

    for df in pd.read_csv("data/OPENDATA_BOOKING_CALL_A_BIKE.csv",
                          chunksize=chunksize,
                          sep=';',
                          iterator=True):

      df = df.rename(columns = {c: c.replace(' ', '') for c in df.columns})
      df.index += j
      df.to_sql('cab_booking', csvdb, if_exists='append')
      j = df.index[-1]+1

      print(f'Index: {j}')

    print("Loaded to sqlite3!\n")

    #Only quering Frankfurt

    data = pd.read_sql_query("SELECT * FROM cab_booking WHERE CITY_RENTAL_ZONE='Frankfurt am Main'", csvdb)

    return data
  
  def extract_data(self):
    
    print("Starting data extraction..")
    df_category = pd.read_csv("data/OPENDATA_CATEGORY_CALL_A_BIKE.csv", sep = ';')
    df_rental = pd.read_csv("data/OPENDATA_RENTAL_ZONE_CALL_A_BIKE.csv", sep = ';')
    df_vehicle = pd.read_csv("data/OPENDATA_VEHICLE_CALL_A_BIKE.csv", sep = ';')

    df_rental = df_rental.drop_duplicates(subset='RENTAL_ZONE_HAL_ID')
    df_vehicle = df_vehicle.drop_duplicates(subset='VEHICLE_HAL_ID')
    df_vehicle = df_vehicle.dropna(axis=1, how='all')
    print("Data extracted..\n")
    return df_category, df_rental, df_vehicle


  def transform_data(self, booking, category, rental, vehicle):

    print("Starting transformation..")
    booking = pd.merge(booking, vehicle, on='VEHICLE_HAL_ID', how='left')
    booking = pd.merge(booking, category,
                       left_on='CATEGORY_HAL_ID', right_on='HAL_ID', how='left')

    booking = booking[booking['START_RENTAL_ZONE_HAL_ID'].notnull() & booking['END_RENTAL_ZONE_HAL_ID'].notnull()]
    booking = booking.drop(['index','TRAVERSE_USE','RENTAL_ZONE_HAL_SRC','COMPANY_x','COMPANY_GROUP_x',
                            'COMPANY_y', 'COMPANY_GROUP_y'], axis=1)

    booking = pd.merge(booking, rental[['RENTAL_ZONE_HAL_ID','LATITUDE', 'LONGITUDE']],
                       left_on='START_RENTAL_ZONE_HAL_ID', right_on='RENTAL_ZONE_HAL_ID', how='inner')
    booking = booking.drop(['RENTAL_ZONE_HAL_ID'], axis=1)
    booking.rename(columns={'LATITUDE' : 'START_RENTAL_LAT', 'LONGITUDE' : 'START_RENTAL_LONG'}, inplace=True)

    booking = pd.merge(booking, rental[['RENTAL_ZONE_HAL_ID','LATITUDE', 'LONGITUDE']],
                       left_on='END_RENTAL_ZONE_HAL_ID', right_on='RENTAL_ZONE_HAL_ID', how='inner')
    booking = booking.drop(['RENTAL_ZONE_HAL_ID'], axis=1)
    booking.rename(columns={'LATITUDE' : 'END_RENTAL_LAT', 'LONGITUDE' : 'END_RENTAL_LONG'}, inplace=True)

    booking = booking[booking['START_RENTAL_LAT'].notnull()]
    booking = booking[booking['END_RENTAL_LAT'].notnull()]

    booking['DATE_FROM'] = pd.to_datetime(booking['DATE_FROM'])
    booking['DATE_UNTIL'] = pd.to_datetime(booking['DATE_UNTIL'])
    booking['START_RENTAL_LAT'] = booking['START_RENTAL_LAT'].apply(lambda x: x.replace(',', '.'))
    booking['START_RENTAL_LONG'] = booking['START_RENTAL_LONG'].apply(lambda x: x.replace(',', '.'))
    booking['END_RENTAL_LAT'] = booking['END_RENTAL_LAT'].apply(lambda x: x.replace(',', '.'))
    booking['END_RENTAL_LONG'] = booking['END_RENTAL_LONG'].apply(lambda x: x.replace(',', '.'))

    booking['TRAVEL_TIME'] = booking['DATE_UNTIL'] - booking['DATE_FROM']
    booking['TRAVEL_TIME'] = booking['TRAVEL_TIME']/np.timedelta64(1,'m')
    booking['DAY_OF_WEEK'] = booking['DATE_FROM'].dt.day_name()
    booking['FROM_TIME'] = booking['DATE_FROM'].dt.time
    booking['UNTIL_TIME'] = booking['DATE_UNTIL'].dt.time
    booking['DATE_FROM'] = booking['DATE_FROM'].dt.date
    booking['DATE_UNTIL'] = booking['DATE_UNTIL'].dt.date
    booking = booking[['BOOKING_HAL_ID', 'CATEGORY_HAL_ID', 'VEHICLE_HAL_ID', 'CUSTOMER_HAL_ID','HAL_ID',
                       'DATE_FROM', 'FROM_TIME', 'DATE_UNTIL', 'UNTIL_TIME','TRAVEL_TIME','DAY_OF_WEEK',
                       'START_RENTAL_ZONE_HAL_ID', 'START_RENTAL_ZONE', 'END_RENTAL_ZONE_HAL_ID', 'END_RENTAL_ZONE',
                       'COMPUTE_EXTRA_BOOKING_FEE', 'CITY_RENTAL_ZONE', 'TECHNICAL_INCOME_CHANNEL', 'VEHICLE_TYPE_NAME',
                       'SERIAL_NUMBER', 'ACCESS_CONTROL_COMPONENT_TYPE','CATEGORY', 'START_RENTAL_LONG',
                       'START_RENTAL_LAT','END_RENTAL_LONG', 'END_RENTAL_LAT']]
    print("Data Transformed!\n")

    return booking

  def load_data(self, booking):
    print("Loading to csv..")
    merged_booking.to_csv("callabike.csv", sep=';')
    print('Done!')

if __name__ == '__main__':

  job = ETL()
  
  booking = job.load_from_sql()
  booking = booking.drop_duplicates(subset='BOOKING_HAL_ID')
  category, rental, vehicle = job.extract_data()
  merged_booking = job.transform_data(booking, category, rental, vehicle)
  job.load_data(merged_booking)