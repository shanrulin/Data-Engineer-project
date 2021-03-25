# Capstone Project: Data Modeling the immigration data with AWS

## Introduction
The goal of this project is to build the immigration data pipelines to create the data model. The data resides in S3, in a directory of SAS7BDAT parquet logs on immigration activities in April, 2016, a CSV file of city temperature, a CSV file of state data, a directory of  CSV file on U.S. City Demographic Data, and an airport data in CSV format.

I build an ETL pipeline that extracts data from S3, processes them using Spark, and loads the data back into S3 as a set of dimensional tables and a fact table. The dataset includes around 6 million lines of data, so the ETL process could still be run on Spark locally but relatively slow. Thus, it would be a good choice to run on AWS EMR. Also, if the dataset include unstructure data such as text or pictures in the future, building ETL on data lake could provide end users with different types of analytics.

## Datasets and process
**State Abbreviations**: The data comes from [here](https://worldpopulationreview.com/states/state-abbreviations). I download the CSV formate and the dataset includes state name, state abbreviation and state code columns. Create a state dimension table from this data.
#### steps
* drop duplicate rows, check if dataset contains 51 states, and save in csv format

**Daily Temperature of Major Cities**: The [dataset](https://www.kaggle.com/sudalairajkumar/daily-temperature-of-major-cities) came from Kaggle and the University of Dayton for making this dataset available in the first place. The data fields in each file posted on this site are: month, day, year, average daily temperature (F). "-99" as a no-data flag when data are not available. I use this data and state table to build a temerature dimension table
#### steps
* create **date** column from **year**, **month**, and **day**
* drop duplicate rows and select subset of the dataset to reduce size
* use spark sql to combine temperature dataset and state table to create a temperature table with state code column, and save in parquet file

**U.S. City Demographic Data**: This [data](https://public.opendatasoft.com/explore/dataset/us-cities-demographics/export/) comes from OpenSoft. Use this data to create a demographic dimension table.
#### steps
* define schema, drop state column, drop duplicate rows, and save in parquet file

**Airport Code Table**: This [dataset](https://datahub.io/core/airport-codes#data) comes from Datahub. Use this data to build a airport dimension table.
#### steps
* define schema, drop continent column, which has many missing values, and drop duplicate ident column
* create **state_code** from **iso_region**, and create **latitude** and **longitude** from **coordinates**
* save airport table to parquet file

**I94 Immigration Data**: This [data](https://travel.trade.gov/research/reports/i94/historical/2016.html) comes from the US National Tourism and Trade Office. A data dictionary called I94_SAS_Labels_Descriptions is included. The dataset is used to build a Fact table that records immigration activities.
#### steps
* convert **arrdate** double type SAS time to date time and rename it as arrival_date
* convert **depdate** double type SAS time to date time and rename it as departure_date
* use spark sql to deal with missing value in **i94mode** and **i94addr**
* check data type of **arrival_date** and **departure_date**
* check if any rows in immigration table


<img src="https://github.com/shanrulin/Data-Engineer-project/blob/main/capstone_ER.PNG">


## Addressing Other Scenarios
* The data was increased by 100x
Data is store in S3, and work on AWS EMR, so one could scale up the hardware configuration.

* The pipelines would be run on a daily basis by 7 am every day
This ETL process could schedule with Airflow to run on a daily basis. Also, since airport dataset, demographic data and state don't change very often, they don't need to run on a daily basis.

* The database needed to be accessed by 100+ people
The data lake is built on AWS S3, so it could handle the increase access to database
