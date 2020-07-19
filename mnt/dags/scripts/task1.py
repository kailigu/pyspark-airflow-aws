import pandas as pd

with open('/opt/data/I94_SAS_Labels_Descriptions.SAS') as f:
    f_content = f.read()
    f_content = f_content.replace('\t', '')


def code_mapper(file, idx):
    f_content2 = f_content[f_content.index(idx):]
    f_content2 = f_content2[:f_content2.index(';')].split('\n')
    f_content2 = [i.replace("'", "") for i in f_content2]
    dic = [i.split('=') for i in f_content2[1:]]
    dic = dict([i[0].strip(), i[1].strip()] for i in dic if len(i) == 2)
    return dic


countries = code_mapper(f_content, "i94cntyl")
cities = code_mapper(f_content, "i94prtl")

# Get table COUNTRY
df_countries = pd.DataFrame.from_dict(countries,orient='index',columns=['COUNTRY_NAME']).reset_index()
df_countries.columns = ['COUNTRY_ID','COUNTRY_NAME']
df_countries.to_parquet('/opt/data/COUNTRY')


# Get field CITY_CODE
df_cities = pd.DataFrame.from_dict(cities,orient='index',columns=['COUNTRY_NAME']).reset_index()
df_cities.columns = ['CITY_CODE','CITY_NAME']
df_cities[['CITY_NAME','STATE_CODE','COUNTRY']] = df_cities['CITY_NAME'].str.split(',',expand=True)
df_cities.drop(df_cities[df_cities['COUNTRY'].notnull()].index, inplace=True)
df_cities.drop(df_cities[df_cities['STATE_CODE'].isnull()].index, inplace=True)
df_cities.drop('COUNTRY', axis=1, inplace=True)
df_cities['CITY_NAME'] = df_cities['CITY_NAME'].str.title()
df_cities.to_parquet('/opt/data/US_CITY_CODE')


# Data Quality check
df = pd.read_parquet("/opt/data/US_CITY_CODE")
if len(df.index) < 1:
    raise ValueError(f"Data quality check failed. It has no records")





