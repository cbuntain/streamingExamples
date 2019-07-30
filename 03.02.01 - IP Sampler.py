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

def sampler(x):
    return random.random() < sample_rate

def norm_counts(rdd):

    if ( not rdd.isEmpty() ):
        summed = rdd.map(lambda t: t[1]).reduce(lambda l,r: l+r)
        return rdd.map(lambda t: (t[0], t[1] / summed)).sortBy(lambda t: t[1], ascending=False)
    else:
        return rdd

# Create a DStream that will connect to hostname:port, like localhost:9999
lines = ssc.socketTextStream(server, port)


# Set the checkpoint directory as necessary for the windowing
ssc.checkpoint("spark_checkpoint")

# Read each line via JSON
ips = lines.flatMap(fw_line_to_ips).filter(lambda t: len(t) > 1).map(lambda t: (t, 1))
sampled_ip_counts = ips.filter(sampler)\
    .window(
        60 * interval,
        1 * interval
    ).reduceByKey(lambda l,r: l+r)

sampled_ip_counts.transform(norm_counts)\
    .pprint()

# sampled_pair_counts.pprint()

ssc.start()             # Start the computation
ssc.awaitTermination()  # Wait for the computation to terminate




