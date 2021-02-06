from pyspark.sql import SparkSession
from pyspark.sql.types import StructType,StructField, StringType, IntegerType ,LongType
from pyspark.sql.types import ArrayType, DoubleType, BooleanType, DateType, TimestampType, DecimalType

import datetime
from typing import List
import datetime
from decimal import Decimal



def parse_csv(line:str):
    """
      Function to parse comma seperate records
      
    """
    
    record_type_pos = 2
    record = line.split(",")
    trade_dt= datetime.datetime.strptime(record[0], '%Y-%m-%d')
    rec_type=record[2]
    symbol=record[3]
    exchange=record[6]
    event_tm=datetime.datetime.strptime(record[4], '%Y-%m-%d %H:%M:%S.%f')
    event_seq_nb=int(record[5])
           
    try:
        if record[record_type_pos] == "T":
            trade_pr=Decimal(record[7])
            event = (trade_dt,rec_type,symbol,exchange,event_tm,event_seq_nb,0,\
                     trade_pr,Decimal('0.0'),0,Decimal('0.0'),0,"T")
            return event
        elif record[record_type_pos] == "Q":
            bid_pr=Decimal(record[7])
            bid_size=int(record[8])
            ask_pr=Decimal(record[9])
            ask_size=int(record[10])

            event = (trade_dt,rec_type,symbol,exchange,event_tm,event_seq_nb,0,\
                     Decimal('0.0'),bid_pr,bid_size,ask_pr,ask_size,"Q")
            return event
    except Exception as e:
      return ("","","","","","","","","","","","B",line)
        
common_event = StructType() \
              .add("trade_dt",DateType(),True) \
              .add("rec_type",StringType(),True) \
              .add("symbol",StringType(),True) \
              .add("exchange",StringType(),True) \
              .add("event_tm",TimestampType(),True) \
              .add("event_seq_nb",IntegerType(),True) \
              .add("arrival_tm",IntegerType(),True) \
              .add("trade_pr",DecimalType(17,14),True) \
              .add("bid_pr",DecimalType(17,14),True) \
              .add("bid_size",IntegerType(),True) \
              .add("ask_pr",DecimalType(17,14),True) \
              .add("ask_size",IntegerType(),True) \
              .add("partition",StringType(),True)
              
## Spark to process the source data 
spark = SparkSession.builder.master('local').\
        appName('app').getOrCreate()


raw = spark.sparkContext.\
           textFile("dbfs:/mnt/FileStore/MountFolder/data/csv/2020-08-05/NYSE/*.txt")

parsed = raw.map(lambda line: parse_csv(line))
data_csv = spark.createDataFrame(parsed,schema=common_event)

data_csv.write.partitionBy("partition").mode("overwrite").parquet("output_dir")