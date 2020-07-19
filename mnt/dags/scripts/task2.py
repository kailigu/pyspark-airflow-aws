import pandas as pd

df_demo = pd.read_csv("/opt/data/us-cities-demographics.csv",sep=";")
df_city = pd.read_parquet("/opt/data/US_CITY_CODE")

# pivot demo to get unique city on each row
df_demo = df_demo.merge(df_city, left_on="City", right_on="CITY_NAME", how="inner")
df_pivot = df_demo.pivot_table(index='City', columns='Race', values='Count')
df_pivot.reset_index(inplace=True)
df_demo.drop(['Race','Count','CITY_NAME'],axis=1, inplace=True)
df_demo.drop_duplicates(subset=None, keep='first', inplace=True)

df_demo = df_demo.merge(df_pivot, left_on="City", right_on="City", how="right")


# get table US_STATE
df_states = df_demo[['STATE_CODE','State']]
df_states.columns = ['STATE_CODE', 'STATE']
df_states.to_parquet('/opt/data/US_STATE')

# get table US_CITY
df_demo.drop(['State','State Code', 'STATE_CODE'],axis=1, inplace=True)
df_demo.columns = df_demo.columns.str.upper()
df_demo.columns = df_demo.columns.str.replace(" ", "_")
df_demo.columns = df_demo.columns.str.replace("-", "_")
df_demo['CITY_ID'] = df_demo.index
df_demo.to_parquet('/opt/data/US_CITY')


# Data Quality check
df = pd.read_parquet("/opt/data/US_CITY")
if len(df.index) < 1:
    raise ValueError(f"Data quality check failed. It has no records")


