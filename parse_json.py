from pyspark.sql import SparkSession
from pyspark.sql.types import StructType,StructField, StringType, IntegerType ,LongType
from pyspark.sql.types import ArrayType, DoubleType, BooleanType, DateType, TimestampType, DecimalType

import json
from decimal import Decimal 
import datetime

def parse_json(line:str):
    """
      Function to parse json records
      
    """
    try:
        # built-in function to parse the json file
        record = json.loads(line)
        
        # Common fields applicable to both 'Q' and 'T' type records
        record_type = record['event_type']
        trade_dt= datetime.datetime.strptime(record['trade_dt'], '%Y-%m-%d')
        arrival_tm=datetime.datetime.strptime(record['file_tm'], '%Y-%m-%d %H:%M:%S.%f')
        rec_type=record_type
        symbol=record['symbol']
        exchange=record['exchange']
        event_tm=datetime.datetime.strptime(record['event_tm'], '%Y-%m-%d %H:%M:%S.%f')
        event_seq_nb=int(record['event_seq_nb'])

        # capturing field values specific to "T" type records
        if record_type == "T":
            trade_pr=Decimal(record['price'])
            event = (trade_dt,rec_type,symbol,exchange,event_tm,event_seq_nb,arrival_tm,\
                     trade_pr,Decimal('0.0'),0,Decimal('0.0'),0,"T")
            return event
        
        # capturing field values specific to "Q" type records
        elif record_type == "Q":
            bid_pr=Decimal(record['bid_pr'])
            bid_size=int(record['bid_size'])
            ask_pr=Decimal(record['ask_pr'])
            ask_size=int(record['ask_size'])
            event = (trade_dt,rec_type,symbol,exchange,event_tm,event_seq_nb,arrival_tm,\
                     Decimal('0.0'),bid_pr,bid_size,ask_pr,ask_size,"Q")
            return event

    # capture record in a bad partition if any error occurs
    except Exception as e:
            return ("","","","","","","","","","","","B",line)



## Spark to process the source data (below line is for local execution)
spark = SparkSession.builder.master('local').appName('app').getOrCreate()

# schema to parse both Q and T type records
common_event = StructType() \
              .add("trade_dt",DateType(),True) \
              .add("rec_type",StringType(),True) \
              .add("symbol",StringType(),True) \
              .add("exchange",StringType(),True) \
              .add("event_tm",TimestampType(),True) \
              .add("event_seq_nb",IntegerType(),True) \
              .add("arrival_tm",TimestampType(),True) \
              .add("trade_pr",DecimalType(17,14),True) \
              .add("bid_pr",DecimalType(17,14),True) \
              .add("bid_size",IntegerType(),True) \
              .add("ask_pr",DecimalType(17,14),True) \
              .add("ask_size",IntegerType(),True) \
              .add("partition",StringType(),True)


raw = spark.sparkContext.textFile("dbfs:/mnt/FileStore/MountFolder/data/json/*/NASDAQ/*.txt")
parsed = raw.map(lambda line: parse_json(line))
data_json = spark.createDataFrame(parsed,schema=common_event)

# Save the final dataframe as parquet files in partitions
data_json.write.partitionBy("partition").mode("overwrite").parquet("dbfs:/mnt/FileStore/MountFolder/output_dir_json")