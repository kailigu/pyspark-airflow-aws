import pandas as pd

df_temp = pd.read_csv("/opt/data/GlobalLandTemperaturesByCity.csv")
df_city = pd.read_parquet("/opt/data/US_CITY_CODE")

# Get CITY_CODE to be joined to fact in task5
df_temp = df_temp.merge(df_city[['CITY_CODE','CITY_NAME']], left_on='City', right_on='CITY_NAME', how='inner')


# Get table WEATHER
df_temp.drop(['CITY_NAME','City'],axis=1, inplace=True)
df_temp.rename(columns={'dt': 'DATE'}, inplace=True)
df_temp['DATE'] = pd.to_datetime(df_temp['DATE'])
df_temp['WEATHER_ID'] = df_temp.index
df_temp.to_parquet('/opt/data/WEATHER')





