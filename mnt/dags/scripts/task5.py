from pyspark.sql import SparkSession
from pyspark.sql.functions import udf
from pyspark.sql.types import DateType
from datetime import datetime, timedelta
import pandas as pd

def convert_datetime(x):
    try:
        start = datetime(1960, 1, 1)
        return start + timedelta(days=int(float(x)))
    except:
        return None
udf_datetime_from_sas = udf(lambda x: convert_datetime(x), DateType())


spark = SparkSession.builder.appName("fact").getOrCreate()
df_fact = spark.read.format('com.github.saurfang.sas.spark')\
                    .load('s3://de-capstone/raw/i94_immigration_data/i94_apr16_sub.sas7bdat')

df_fact = df_fact.withColumn("ARRIVE_DATE", udf_datetime_from_sas("arrdate")) \
                .withColumn("DEP_DATE", udf_datetime_from_sas("depdate"))


# Join with AIRPORT to add AIRPORT_ID to fact table
df_airport = spark.read.parquet('/opt/data/AIRPORT').select('AIRPORT_ID','CITY_CODE')
df_fact = df_fact.join(df_airport, df_fact.i94port == df_airport.CITY_CODE, how='inner')
df_fact = df_fact.drop("CITY_CODE","city")


# Join with WEATHER to add WEATHER_ID to fact table
df_temp = spark.read.parquet('/opt/data/WEATHER').select("DATE","CITY_CODE","WEATHER_ID")
df_fact = df_fact.join(df_temp, (df_temp.DATE == df_fact.ARRIVE_DATE) & (df_temp.CITY_CODE == df_fact.i94port), how="left")\
                .drop("DATE","CITY_CODE").withColumnRenamed("WEATHER_ID","ARRIVE_WEATHER_ID")
df_fact = df_fact.join(df_temp, (df_temp.DATE == df_fact.DEP_DATE) & (df_temp.CITY_CODE == df_fact.i94port), how="left")\
                .drop("DATE","CITY_CODE").withColumnRenamed("WEATHER_ID","DEP_WEATHER_ID")


# Join with CITY to add CITY_ID to fact table
df_city = spark.read.parquet('/opt/data/US_CITY').select('CITY_ID','CITY_CODE')
df_fact = df_fact.join(df_city, df_fact.i94port == df_city.CITY_CODE, how="inner")


# Clean and write fact table 
df_fact = df_fact.drop("_c0","i94yr","i94mon","i94res","i94port","i94addr","arrdate","depdate","CITY_CODE",\
                      "dtadfile","visapost","entdepa","entdepd","entdepu","matflag","dtaddto","insnum")
df_fact = df_fact.selectExpr("cicid as IMMI_ID","i94cit as COUNTRY_ID","CITY_ID","AIRPORT_ID","ARRIVE_WEATHER_ID",\
                   "DEP_WEATHER_ID", "ARRIVE_DATE","DEP_DATE","i94mode as TRAVEL_MODE", "i94bir as AGE",\
                   "i94visa as VISA_CODE", "occup as OCCUPATION" ,"biryear as BIRTH_YEAR", "gender as GENDER",\
                   "airline as AIRLINE","admnum as ADMISSION_CODE","fltno as FLIGHT_CODE", "visatype as VISA_TYPE" ) 
df_fact = df_fact.toPandas()                               
df_fact.to_csv('/opt/bitnami/spark/output/IMMIGRATION.csv')


# Data Quality check
df_fact = pd.read_csv("/opt/bitnami/spark/output/IMMIGRATION.csv")
if len(df_fact.index) < 1:
    raise ValueError(f"Data quality check on IMMIGRATION failed. It has no records")

