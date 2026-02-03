#!/usr/bin/env python
# coding: utf-8

# In[ ]:


from pyspark import SparkConf,SparkContext
from pyspark.streaming import StreamingContext
from pyspark.sql import Row,SQLContext
import sys
import requests
import time

def aggregate_tags_count(new_values, total_sum):
    return sum(new_values) + (total_sum or 0)

def get_sql_context_instance(spark_context):
    if ('sqlContextSingletonInstance' not in globals()):
            globals()['sqlContextSingletonInstance'] = SQLContext(spark_context)
    return globals()['sqlContextSingletonInstance']



def process_rdd(time, rdd):
    #print("----------- %s -----------" % str(time))
    #print(rdd)
    #print(rdd.context)
    try:
       # Get spark sql context from the current context
        sql_context = get_sql_context_instance(rdd.context)
        #print("Get spark sql context -- %s -------" % str(time))
    
        # convert the RDD to Row RDD
        row_rdd = rdd.map(lambda w: Row(word=w))
        # word=w[0], word_count=w[1]))
        #print("rdd done")   
        #print(row_rdd.take(10))
        # create a DF from the Row RDD
        new_df = sql_context.createDataFrame(row_rdd)
        # Register the dataframe as table
        #print("df done")
    except:
        pass



# create spark configuration
conf = SparkConf()
conf.setAppName("TwitterStreamApp")

# create spark context with the above configuration
sc = SparkContext(conf=conf)
sc.setLogLevel("ERROR")

# create the Streaming Context from the above spark context with interval size 2 seconds
ssc = StreamingContext(sc,.5)

# setting a checkpoint to allow RDD recovery
ssc.checkpoint("checkpoint_TwitterApp")

# read data from port 9009
dataStream = ssc.socketTextStream("127.0.1.1",9000)
#print("DATA STREAMMMMM")
print(dataStream.pprint())

# split each tweet into words
words = dataStream.flatMap(lambda line: line.split(" "))
#print("WORRRRRRRRRRRRRRRRRRRRRRRRDSSSSSSSSSSS")
#print(words.pprint())
# filter the words to get only hashtags, then map each hashtag to be a pair of (hashtag,1)
hashtags = words.map(lambda x: (x, 1)) 
#print("#######################")
#print(hashtags.pprint())
# adding the count of each hashtag to its last count
tags_totals = hashtags.updateStateByKey(aggregate_tags_count)
#print("###")
#print(tags_totals.pprint())
# do processing for each RDD generated in each interval
tags_totals.foreachRDD(process_rdd)


# start the streaming computation
ssc.start()

# wait for the streaming to finish
time.sleep(300)
ssc.stop()

#ssc.awaitTermination()

