import os
from pyspark.sql import functions as F
from pyspark.sql import SparkSession
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
enugu_df=enugu_df.join(location_df,"LocationID",how="left").drop("No of ATMs","Country","LocationID").withColumn(
    "duration_sec",
    F.unix_timestamp("TransactionEndDateTime") - F.unix_timestamp("TransactionStartDateTime")
).join(transaction_type_df,"TransactionTypeID",how="left").join(customer_df,"CardholderID",how="left").drop("IsWisabi","First Name","Last Name","Birth Date")
#### logging for debugging and validation
enugu_df.show()
enugu_df.printSchema()
### fct_df and location_df:
fct_df=(fct_df.join(location_df,"LocationID",how="left").drop("No of ATMs","Country","LocationID").withColumn(
    "duration_sec",
    F.unix_timestamp("TransactionEndDateTime","MM/dd/yyyy HH:mm") - F.unix_timestamp("TransactionStartDateTime","MM/dd/yyyy HH:mm"))
        .join(transaction_type_df,"TransactionTypeID",how="left")).join(customer_df,"CardholderID",how="left").drop("IsWisabi","First Name","Last Name","Birth Date")
#### logging for debugging and validation
fct_df.show()
fct_df.printSchema()
### kan_df and location_df:
kan_df=(kan_df.join(location_df,"LocationID",how="left").drop("No of ATMs","Country","LocationID").withColumn(
    "duration_sec",
    F.unix_timestamp("TransactionEndDateTime","MM/dd/yyyy HH:mm") - F.unix_timestamp("TransactionStartDateTime","MM/dd/yyyy HH:mm"))
        .join(transaction_type_df,"TransactionTypeID",how="left")).join(customer_df,"CardholderID",how="left").drop("IsWisabi","First Name","Last Name","Birth Date")
#### logging for debugging and validation
kan_df.show()
kan_df.printSchema()
### river_df and location_df:
river_df=(river_df.join(location_df,"LocationID",how="left").drop("No of ATMs","Country","LocationID").withColumn(
    "duration_sec",
    F.unix_timestamp("TransactionEndDateTime","MM/dd/yyyy HH:mm") - F.unix_timestamp("TransactionStartDateTime","MM/dd/yyyy HH:mm"))
          .join(transaction_type_df,"TransactionTypeID",how="left")).join(customer_df,"CardholderID",how="left").drop("IsWisabi","First Name","Last Name","Birth Date")
#### logging for debugging and validation
river_df.show()
river_df.printSchema()
### lagos_df and location_df:
lagos_df=(lagos_df.join(location_df,"LocationID",how="left").drop("No of ATMs","Country","LocationID").withColumn(
    "duration_sec",
    F.unix_timestamp("TransactionEndDateTime","MM/dd/yyyy HH:mm") - F.unix_timestamp("TransactionStartDateTime","MM/dd/yyyy HH:mm"))
          .join(transaction_type_df,"TransactionTypeID",how="left")).join(customer_df,"CardholderID",how="left").drop("IsWisabi","First Name","Last Name","Birth Date")
#### logging for debugging and validation
lagos_df.show()
lagos_df.printSchema()
#######################################################################################################

