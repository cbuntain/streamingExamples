#!/usr/bin/env python
# coding: utf-8

# # Spark Streaming Example
# 
# Modified from https://spark.apache.org/docs/latest/streaming-programming-guide.html#a-quick-example

import sys
import json

from pyspark.sql import SparkSession
from pyspark.streaming import StreamingContext

# Import data types
from pyspark.sql.types import *

spark = SparkSession.builder.appName('streaming_app').getOrCreate()

sc = spark.sparkContext
sc.setLogLevel("ERROR")

interval = int(sys.argv[1])
server = sys.argv[2]
port = int(sys.argv[3])

ssc = StreamingContext(sc, interval)

def fw_line_to_ips(line):
    entry = json.loads(line)
    
    if entry["Source IP"] == "(empty)":
        return []
    elif entry["Destination IP"] == "(empty)":
        return (entry["Source IP"], )
    else:
        return (entry["Source IP"], entry["Destination IP"])

# Create a DStream that will connect to hostname:port, like localhost:9999
lines = ssc.socketTextStream(server, port)

# Read each line via JSON
ips = lines.flatMap(fw_line_to_ips).pprint()

ssc.start()             # Start the computation
ssc.awaitTermination()  # Wait for the computation to terminate


