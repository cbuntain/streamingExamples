#!/usr/bin/env python
# coding: utf-8

# # Spark Streaming Example
# 
# Modified from https://spark.apache.org/docs/latest/streaming-programming-guide.html#a-quick-example

import sys
import json
import random
import pandas as pd

from pyspark.sql import SparkSession
from pyspark.streaming import StreamingContext

spark = SparkSession.builder.appName('streaming_app').getOrCreate()

sc = spark.sparkContext
sc.setLogLevel("ERROR")

interval = int(sys.argv[1])
server = sys.argv[2]
port = int(sys.argv[3])
sample_rate = float(sys.argv[4])

ssc = StreamingContext(sc, interval)

def fw_line_to_ips(line):
    entry = json.loads(line)
    
    if entry["Source IP"] == "(empty)":
        return []
    elif entry["Destination IP"] == "(empty)":
        return (entry["Source IP"], )
    else:
        return tuple(sorted([entry["Source IP"], entry["Destination IP"]]))

def sampler_v2(x):
    return (hash(x) % 100) < (sample_rate * 100)

# Create a DStream that will connect to hostname:port, like localhost:9999
lines = ssc.socketTextStream(server, port)


# Set the checkpoint directory as necessary for the windowing
ssc.checkpoint("spark_checkpoint")

# Read each line via JSON
ips = lines.map(fw_line_to_ips).filter(lambda t: len(t) > 1).map(lambda t: (t, 1))
sampled_pair_counts = ips.filter(sampler_v2)\
    .window(
        60 * interval,
        1 * interval
    ).reduceByKey(lambda l,r: l+r)

sampled_pair_counts.map(lambda tup: (1, 1 if tup[1] > 1 else 0))\
    .reduce(lambda l, r: (l[0] + r[0], l[1] + r[1]))\
    .map(lambda t: t[1] / t[0])\
    .pprint()

# sampled_pair_counts.pprint()

ssc.start()             # Start the computation
ssc.awaitTermination()  # Wait for the computation to terminate




