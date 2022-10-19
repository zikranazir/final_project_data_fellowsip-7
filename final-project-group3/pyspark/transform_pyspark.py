import os
import glob
import datetime
# import findspark
import pandas as pd
#import gcsfs
from pyspark import SparkContext, SparkConf
from datetime import timedelta
from pyspark.sql.functions import udf
from pyspark.sql import SparkSession
from pyspark.sql import types
from google.cloud import storage

bucket = "group3_data-fellowship-batch-7"
file_name = "credit_card_data"



def transform():
    # Create Spark Session
    spark = SparkSession.builder \
            .master("local[*]") \
            .appName("Credict card analysis") \
            .config('spark.executor.memory', '5gb') \
            .config("spark.cores.max", "6") \
            .getOrCreate()

    # spark._jsc.hadoopConfiguration().set('fs.gs.impl', 'com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystem')
    # # This is required if you are using service account and set true, 
    # spark._jsc.hadoopConfiguration().set('fs.gs.auth.service.account.enable', 'true')
    # spark._jsc.hadoopConfiguration().set('google.cloud.auth.service.account.json.keyfile', "/home/hidayatullohalfian/final-project-group3/airflow2/google_credentials.json")



    #path to dataset
    application_record = pd.read_csv("gs://group3_data-fellowship-batch-7/application_record")
    credit_record = pd.read_csv("gs://group3_data-fellowship-batch-7/credit_record")


    credit_schema = types.StructType(
        [
            types.StructField('ID', types.StringType(), True),
            types.StructField('months_balance', types.StringType(), True),
            types.StructField('status', types.StringType(), True)
        ]
    )


    # format credit_record to sparkDF
    credit_df = spark.createDataFrame(credit_record, schema=credit_schema) 


    application_schema = types.StructType(
        [
            types.StructField('ID', types.IntegerType(), True),
            types.StructField('CODE_GENDER', types.StringType(), True),
            types.StructField('FLAG_OWN_CAR', types.StringType(), True),
            types.StructField('FLAG_OWN_REALTY', types.StringType(), True),
            types.StructField('CNT_CHILDREN', types.IntegerType(), True),
            types.StructField('AMT_INCOME_TOTAL', types.DoubleType(), True),
            types.StructField('NAME_INCOME_TYPE', types.StringType(), True),
            types.StructField('NAME_EDUCATION_TYPE', types.StringType(), True),
            types.StructField('NAME_FAMILY_STATUS', types.StringType(), True),
            types.StructField('NAME_HOUSING_TYPE', types.StringType(), True),
            types.StructField('DAYS_BIRTH', types.IntegerType(), True),
            types.StructField('DAYS_EMPLOYED', types.IntegerType(), True),
            types.StructField('FLAG_MOBIL', types.IntegerType(), True),
            types.StructField('FLAG_WORK_PHONE', types.IntegerType(), True),
            types.StructField('FLAG_PHONE', types.IntegerType(), True),
            types.StructField('FLAG_EMAIL', types.IntegerType(), True),
            types.StructField('OCCUPATION_TYPE', types.StringType(), True),
            types.StructField('CNT_FAM_MEMBERS', types.DoubleType(), True)
        ]
    )

    application_df = spark.createDataFrame(application_record, schema=application_schema) #format credit_record to sparkDF

    print("Transform On Proccess")
    print("######################")

    cl_application_df = application_df \
        .withColumn("CNT_FAMILY_MEMBERS", application_df.CNT_CHILDREN + application_df.CNT_FAM_MEMBERS) \
        .drop("CNT_CHILDREN", "CNT_FAM_MEMBERS", "FLAG_MOBIL", "FLAG_WORK_PHONE", "FLAG_PHONE", "FLAG_EMAIL")


    cl_application_df = cl_application_df \
        .withColumn("CNT_FAMILY_MEMBERS", cl_application_df.CNT_FAMILY_MEMBERS.cast('int'))


    def cat_transofrmation():
        # Cleaning up categorical values to lower the count of dummy variables.
        income_type = {'Commercial associate':'Working',
                    'State servant':'Working',
                    'Working':'Working',
                    'Pensioner':'Pensioner',
                    'Student':'Student'}
        education_type = {'Secondary / secondary special':'secondary',
                        'Lower secondary':'secondary',
                        'Higher education':'Higher education',
                        'Incomplete higher':'Higher education',
                        'Academic degree':'Academic degree'}
        family_status = {'Single / not married':'Single',
                        'Separated':'Single',
                        'Widow':'Single',
                        'Civil marriage':'Married',
                        'Married':'Married'}

        data = cl_application_df.replace(to_replace=income_type, subset=['NAME_INCOME_TYPE'])
        data = cl_application_df.replace(to_replace=education_type, subset=['NAME_EDUCATION_TYPE'])
        data = cl_application_df.replace(to_replace=family_status, subset=['NAME_FAMILY_STATUS'])
        return data


    credit_data = cat_transofrmation()


    ## This function takes no of days and convert it into their datetime format
    def to_time(total_days):
        today = datetime.date.today()
        time = (today + timedelta(days=total_days)).strftime('%Y-%m-%d')
        return time                  

    to_age = udf(lambda x: to_time(x))
    to_employed = udf(lambda x : to_time(x))


    df = credit_data.withColumn("Age", to_age("DAYS_BIRTH")).drop("DAYS_BIRTH")
    credit_data = df.withColumn("TIME_WORK", to_employed("DAYS_EMPLOYED")).drop("DAYS_EMPLOYED")


    # let's create a function to calculate age of the employee
    def convert_date(date):
        today=datetime.date.today()
        time=datetime.datetime.strptime(date,'%Y-%m-%d')
        lengt_time=today.year-time.year
        return lengt_time

    age = udf(lambda x: convert_date(x))
    employed = udf(lambda x : convert_date(x))



    df1 = credit_data.withColumn("AGES", age("Age"))
    credit_data = df1.withColumn("TIME_EMPLOYED", employed("TIME_WORK"))


    credit_card_data = credit_data.join(credit_df, ["ID"], "inner")

    # Write Data into Parquet
    credit_card_data.coalesce(1).write.format("parquet").mode("overwrite").save(f"{file_name}")
    
    print("Transform Already Done and parquet file has written in folder {}".format(file_name))
    print("##################")

    # Send Data to GCS
    print("### Send Parquet File to GCS ###")
    path = f"/home/hidayatullohalfian/final-project-group3/{file_name}"
    os.chdir(path)
    part = 1
    client = storage.Client()
    gcs_bucket = client.get_bucket(bucket)

    for file in glob.glob('*.parquet'):
        blob = gcs_bucket.blob(f"{part}_{file_name}")
        blob.upload_from_filename(file)

    print("### DONE! ###")