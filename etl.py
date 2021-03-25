#!/usr/bin/env python
# coding: utf-8

# In[1]:


import os
from pyspark.sql import SparkSession
import pyspark.sql.functions as F
from pyspark.sql.types import StructType, StructField as Fld, DoubleType as Dbl, StringType as Str, IntegerType as Int
from datetime import datetime, timedelta
from pyspark.sql import types as T
from pyspark.sql.functions import udf


# In[ ]:


def create_spark_session():
    '''build spark interface'''
    
    spark = SparkSession\
                .builder\
                .appName("project_trial")\
                .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:2.7.0")\
                .getOrCreate()
    return spark


# In[ ]:


def convert_datetime(x):
    '''convert SAS time to datetime'''
    try:
        start = datetime(1960, 1, 1)
        return start + timedelta(days=int(x))
    except:
        return None


# In[ ]:


def data_type_check(dataframe, col, exp_type):
    """
    Check data type
    
    Parameters
    dataframe :   
        pyspark dataframe
    col : str
        column that need to be checked
    exp_type : str  
        expected data type
    """
    
    col_type = dict(dataframe.dtypes)[col]
    
    if col_type == exp_type:
        print("{} pass data type check".format(col))
    else:
        print("data type issue with {}".format(col))


# In[ ]:


def process_state_data(spark, input_data, output_data):
    """ 
    reads state data from S3, processes that data using Spark to build tables, and writes them back to S3
    Parameters: 
    spark      : spark interface
    input_data : S3 input bucket
    output_data: S3 output bucket
    
    return state dataframe
    """
    
    # get filepath to state data file
    state_data = os.path.join(input_data, 'state.csv')
    
    # read state data file
    spark_state = spark.read.format('csv').option("header", 'True').load(state_data)

    # Clean data and perform data quality check
    spark_state = spark_state.dropDuplicates()
    state_num = spark_state.select('State').count()
    if state_num == 51:
        print("Pass quality check")
        print("State table contains {} states".format(state_num))
    else:
        print("Issue with State data")
    
    # write state table to csv files 
    spark_state.write.mode('overwrite').csv(os.path.join(output_data,'state.csv'), header='true')
    
    return spark_state


# In[ ]:


def process_temperature_data(spark, input_data, output_data, spark_state):
    """ 
    reads temperature data from S3, processes that data using Spark to build tables, and writes them back to S3
    Parameters: 
    spark      : spark interface
    input_data : S3 input bucket
    output_data: S3 output bucket
    """
    
    # get filepath to temperature data file
    temp_data = os.path.join(input_data, 'city_temperature.csv')
    
    # read temperature data file
    spark_temp = spark.read.format("csv").option("header", "true").load(temp_data)

    # create date
    spark_temp = spark_temp.withColumn("date", F.to_date(F.concat_ws("-", "Year", "Month", "Day")) )
    # change temperature's data type from string to double
    spark_temp = spark_temp.withColumn("AvgTemperature",spark_temp.AvgTemperature.cast(Dbl()))
    # clean duplicate rows
    spark_temp = spark_temp.dropDuplicates()
    spark_temp_cleaned = spark_temp.filter((spark_temp['Country'] == "US") & (spark_temp['Year'] >= "2015") )
   
    # create sql view
    spark_temp_cleaned.createOrReplaceTempView("temperature")
    spark_state.createOrReplaceTempView("state")
       
    # get temperature table
    temp_table = spark.sql("""
        SELECT Country, s.code as state_code, city, AvgTemperature, date
        FROM temperature as t
        JOIN state as s
          ON s.state=t.state""")
    
    # write temperature table to parquet files 
    temp_table.write.mode("overwrite").parquet(os.path.join(output_data, 'temperature'))
    print("finish Temperature data!")


# In[ ]:


def process_demographics_data(spark, input_data, output_data):
    """ 
    reads demographics data from S3, processes that data using Spark to build tables, and writes them back to S3
    Parameters: 
    spark      : spark interface
    input_data : S3 input bucket
    output_data: S3 output bucket
    """
    
    # get filepath to demographics data file
    dem_data = os.path.join(input_data, "us-cities-demographics.csv")
    
    # define schema
    schema = StructType([\
        Fld("City", Str()), Fld("State", Str()), Fld("Median_Age", Dbl()), Fld("Male_Population", Int()),\
        Fld("Female_Population", Int()), Fld("Total_Population", Int()), Fld("Number_of_Veterans", Int()),\
        Fld("Foreign_born", Int()), Fld("Average_Household_Size", Dbl()), Fld("State_Code", Str()),\
        Fld("Race", Str()), Fld("Count", Int())])
    
    # read demographics data file
    spark_dem = spark.read.format("csv").option("header", "true").schema(schema).option("delimiter", ";").load(dem_data)

    # Clean data 
    spark_dem = spark_dem.drop("State")
    spark_dem = spark_dem.dropDuplicates()
    
    # write state table to csv files 
    spark_dem.write.mode("overwrite").parquet(os.path.join(output_data, 'demographics'))   
    print("finish demographic data")


# In[ ]:


def process_airport_data(spark, input_data, output_data):
    """ 
    reads airport data from S3, processes that data using Spark to build tables, and writes them back to S3
    Parameters: 
    spark      : spark interface
    input_data : S3 input bucket
    output_data: S3 output bucket
    """
    
    # get filepath to airport data file
    airport_data = os.path.join(input_data, "airport-codes_csv.csv")
    
    # create schema
    schema = StructType([\
        Fld("ident", Str(), False), Fld("type", Str()), Fld("name", Str()), Fld("elevation_ft", Dbl()),\
        Fld("continent", Str()), Fld("iso_country", Str()), Fld("iso_region", Str()), Fld("municipality", Str()),\
        Fld("gps_code", Str()), Fld("iata_code", Str()), Fld("local_code", Str()), Fld("coordinates", Str())])
    
    # read airport data file
    spark_airport = spark.read.csv(airport_data, schema=schema)

    # filter and clean data
    spark_airport_cleaned = spark_airport.drop("continent").filter(spark_airport.iso_country == "US").dropDuplicates(["ident"])
    
    # create state code, latitude and longitude
    spark_airport_cleaned = spark_airport_cleaned.withColumn('state_code', F.split(spark_airport_cleaned['iso_region'], '-').getItem(1))            .withColumn('latitude', F.split(spark_airport_cleaned['coordinates'], ',').getItem(0).cast(Dbl()))            .withColumn('longitude', F.split(spark_airport_cleaned['coordinates'], ',').getItem(1).cast(Dbl()))
    
    spark_airport_cleaned = spark_airport_cleaned.drop("iso_region", "coordinates")
    
    # write airport table to parquet files 
    spark_airport_cleaned.write.mode("overwrite").parquet(os.path.join(output_data, 'airport'))
    print("finish airport table")


# In[ ]:


def process_immigration_data(spark, input_data, output_data):
    """ 
    reads immigration data from S3, processes that data using Spark to build tables, and writes them back to S3
    Parameters: 
    spark      : spark interface
    input_data : S3 input bucket
    output_data: S3 output bucket
    """
    
    # get filepath to immigration data file
    immigration_data = os.path.join(input_data, 'sas_data')
    
    # Read the partitioned table
    mergedDF = spark.read.option("mergeSchema", "true").parquet(immigration_data)
    
    ## convert double type SAS time to date time
    udf_datetime_from_sas = udf(lambda x: convert_datetime(x), T.DateType())
    mergedDF_fixed = mergedDF.withColumn("arrival_date", udf_datetime_from_sas("arrdate"))\
                             .withColumn("departure_date", udf_datetime_from_sas("depdate"))
    
    # create table 
    mergedDF_fixed.createOrReplaceTempView("immigrations")
    
    # get immigration table
    df_immigration = spark.sql("""
    SELECT cicid,
           i94yr,
           i94mon,
           i94cit as origin,
           i94res as resident,
           i94port as port_code,
           arrival_date,
           IFNULL(i94mode, 'Not reported') as mode,
           IFNULL(i94addr, '99') as state,
           departure_date,
           i94bir as age,
           i94visa as purpose,
           dtadfile as file_date,
           visapost as visa_issued_department,
           occup as occupation,
           entdepa as arrival_flag,
           entdepd as departure_flag,
           entdepu as update_flag,
           matflag as match_flag,
           biryear,
           dtaddto as admitted_date,
           gender,
           insnum as INS_number,
           airline,
           admnum,
           fltno as flight_number,
           visatype
    FROM immigrations""")
    
    # check data type for arrival date and departure date
    data_type_check(df_immigration, 'arrival_date', 'date')
    data_type_check(df_immigration, 'departure_date', 'date')
    
    df_immigration = df_immigration.dropDuplicates(['cicid'])
    
    # check records in immigration data
    row_num = df_immigration.count()
    if row_num == 0:
        print("no records in immigration table")
    else:
        print("table immigration has {} rows".format(row_num)) 
    
    # write immigration table to parquet files 
    df_immigration.write.mode("overwrite").parquet(os.path.join(output_data, 'immigration'))
    
    print("finish immigration table")


# In[ ]:


def main():
    spark = create_spark_session()
    input_data = "s3a://shan-project-data/"
    output_data = "s3a://shan-project-out/"
    
    spark_state = process_state_data(spark, input_data, output_data)    
    process_temperature_data(spark, input_data, output_data, spark_state)
    process_demographics_data(spark, input_data, output_data)    
    process_airport_data(spark, input_data, output_data)
    process_immigration_data(spark, input_data, output_data)
    
    spark.stop()

if __name__ == "__main__":
    main()

