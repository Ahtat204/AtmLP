import os
import pyspark.sql.functions  as functions
from pyspark.sql import SparkSession
from  pyspark.sql.window import Window
from pyspark.sql.dataframe import DataFrame


Historical_Data_Path=os.getenv("Historical_Data_Path")
spark = SparkSession.builder.appName("Spark_Batch_Processor").getOrCreate()
spark.sparkContext.setLogLevel("ERROR")
spark.conf.set("spark.sql.legacy.timeParserPolicy", "LEGACY")
enugu=f"{Historical_Data_Path}/enugu_transactions.csv"
fct=f"{Historical_Data_Path}/fct_transactions.csv"
kano=f"{Historical_Data_Path}/kano_transactions.csv"
river=f"{Historical_Data_Path}/rivers_transactions.csv"
lagos=f"{Historical_Data_Path}/lagos_transactions.csv"
location=f"{Historical_Data_Path}/atm_location_lookup.csv"
calendar=f"{Historical_Data_Path}/calendar_lookup.csv"
customer=f"{Historical_Data_Path}/customers_lookup.csv"
transaction_type=f"{Historical_Data_Path}/transaction_type_lookup.csv"
#time=f"{Historical_Data_Path}/hour_lookup.csv"
enugu_df = spark.read.csv(enugu, header=True, inferSchema=True)
fct_df = spark.read.csv(fct, header=True, inferSchema=True)
kan_df = spark.read.csv(kano, header=True, inferSchema=True)
river_df = spark.read.csv(river, header=True, inferSchema=True)
lagos_df = spark.read.csv(lagos, header=True, inferSchema=True)
location_df = spark.read.csv(location, header=True, inferSchema=True)
customer_df = spark.read.csv(customer, header=True, inferSchema=True)
transaction_type_df = spark.read.csv(transaction_type, header=True, inferSchema=True)
calendar_df = spark.read.csv(calendar, header=True, inferSchema=True)
## left join with location_df to Add location,and with transaction_type_df to add Transactiontype :
### enugu_df and location_df:
enugu_df:DataFrame=enugu_df.join(location_df,"LocationID",how="left").drop("No of ATMs","Country","LocationID").withColumn(
    "duration_sec",
    functions.unix_timestamp("TransactionEndDateTime", "MM/dd/yyyy HH:mm") - functions.unix_timestamp("TransactionStartDateTime",
                                                                                      "MM/dd/yyyy HH:mm")
).join(transaction_type_df,"TransactionTypeID",how="left").join(customer_df,"CardholderID",how="left").drop("IsWisabi","First Name","Last Name","Birth Date")
### fct_df and location_df:
fct_df:DataFrame=((fct_df.join(location_df,"LocationID",how="left").drop("No of ATMs","Country","LocationID")
        .withColumn("duration_sec",
                    functions.unix_timestamp("TransactionEndDateTime", "MM/dd/yyyy HH:mm") - functions.unix_timestamp("TransactionStartDateTime", "MM/dd/yyyy HH:mm"))
        .join(transaction_type_df,"TransactionTypeID",how="left")).join(customer_df,"CardholderID",how="left")
        .drop("IsWisabi","First Name","Last Name","Birth Date"))
### kan_df and location_df:
kan_df:DataFrame=(kan_df.join(location_df,"LocationID",how="left").drop("No of ATMs","Country","LocationID").withColumn(
    "duration_sec",
    functions.unix_timestamp("TransactionEndDateTime", "MM/dd/yyyy HH:mm") - functions.unix_timestamp("TransactionStartDateTime", "MM/dd/yyyy HH:mm"))
        .join(transaction_type_df,"TransactionTypeID",how="left")).join(customer_df,"CardholderID",how="left").drop("IsWisabi","First Name","Last Name","Birth Date")
### river_df and location_df:
river_df:DataFrame=(river_df.join(location_df,"LocationID",how="left").drop("No of ATMs","Country","LocationID").withColumn(
    "duration_sec",
    functions.unix_timestamp("TransactionEndDateTime", "MM/dd/yyyy HH:mm") - functions.unix_timestamp("TransactionStartDateTime", "MM/dd/yyyy HH:mm"))
          .join(transaction_type_df,"TransactionTypeID",how="left")).join(customer_df,"CardholderID",how="left").drop("IsWisabi","First Name","Last Name","Birth Date")
### lagos_df and location_df:
lagos_df:DataFrame=(lagos_df.join(location_df,"LocationID",how="left").drop("No of ATMs","Country","LocationID").withColumn(
    "duration_sec",
    functions.unix_timestamp("TransactionEndDateTime", "MM/dd/yyyy HH:mm") - functions.unix_timestamp("TransactionStartDateTime", "MM/dd/yyyy HH:mm"))
          .join(transaction_type_df,"TransactionTypeID",how="left")).join(customer_df,"CardholderID",how="left").drop("IsWisabi","First Name","Last Name","Birth Date")

#######################################################################################################

##merging all 5 by-location dataframes:
df:DataFrame=enugu_df.unionByName(fct_df).unionByName(kan_df).unionByName(river_df).unionByName(lagos_df).withColumnRenamed("TransactionTypeName","TransactionType")
## aggregations
df_by_state_type:DataFrame=df.groupBy("State","TransactionType").agg(functions.count("*").alias("nb_transactions"),
                                                                     functions.avg("TransactionAmount").alias("montant_moyen_ngn"),
                                                                     functions.sum("TransactionAmount").alias("volume_totale_ngn"),
                                                                     functions.max("TransactionAmount").alias("montant_max_ngn"),)
df_by_state_type.show()
## the TOP 5 most solicited ATMs
top_five_atm=df.groupBy("ATMID","State","City").agg(functions.count("*").alias("nb_transactions"),functions.sum("TransactionAmount").alias("total_retire"))
top_five_atm.show()
top_five_atm.printSchema()
window=Window.partitionBy("State").orderBy("total_volume")
print(type(window),window.__class__.__module__)
## TOP 3 ATMs/State
top_3_atm_per_state=df.groupBy("ATMID","State"). agg(functions.sum("TransactionAmount").  alias("total_volume"),functions.count("*").alias("nb_transactions")).withColumn("dense_rank", functions.dense_rank().over(window))
top_3_atm_per_state.show()