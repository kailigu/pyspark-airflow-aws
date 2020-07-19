import pandas as pd

df_airport = pd.read_csv("/opt/data/airport-codes_csv.csv")
df_city = pd.read_parquet("/opt/data/US_CITY_CODE")

# Get CITY_CODE to be joined to fact in task5
df_airport = df_airport.merge(df_city[['CITY_CODE']], left_on='local_code', right_on='CITY_CODE', how='inner')


# Get table AIRPORT
df_airport.drop(['gps_code','iata_code','local_code'],axis=1, inplace=True)
df_airport.rename(columns={'ident': 'AIRPORT_ID'}, inplace=True)
df_airport.columns = df_airport.columns.str.upper()
df_airport.to_parquet('/opt/data/AIRPORT')


# Data Quality check
df = pd.read_parquet("/opt/data/AIRPORT")
if len(df.index) < 1:
    raise ValueError(f"Data quality check failed. It has no records")



