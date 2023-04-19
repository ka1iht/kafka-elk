from kafka import KafkaConsumer, KafkaProducer
import spacy
import pyspark
from pyspark import SparkContext
from pyspark.sql import SparkSession
import json

sc = SparkContext()
spark = SparkSession(sc)

nlp = spacy.load("en_core_web_sm")

consumer = KafkaConsumer('raw', bootstrap_servers = ['localhost: 9092'], auto_offset_reset = 'earliest', enable_auto_commit = True)
producer = KafkaProducer(value_serializer = lambda x: json.dumps(x).encode('utf-8'), bootstrap_servers = ['localhost: 9092'])

words = []

for message in consumer:
    # print("Received:", message.value.decode())
    doc = nlp(message.value.decode())
    for entities in doc.ents:
        if not (entities.label_ == "ORDINAL" or entities.label_ == "CARDINAL" or entities.label_ == "PERCENT"):
            words.append(entities.text)
    wordsRDD = sc.parallelize(words)
    mapOutput = wordsRDD.map(lambda x: (x,1))
    mapOutput = mapOutput.reduceByKey(lambda x,y: x+y).sortBy(lambda x: -x[1])
    print("-----------------------------------------------------------------------------------------------------------------")
    print(mapOutput.take(10))
    jsonDump = {}
    for element in mapOutput.take(10):
        jsonDump["word"] = element[0]
        jsonDump["count"] = element[1]
        producer.send('words', jsonDump)
