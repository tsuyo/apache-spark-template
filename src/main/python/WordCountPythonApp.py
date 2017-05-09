"""WordCountPythonApp.py"""
from pyspark import SparkContext

inputFile = "/opt/spark-2.1.1-bin-hadoop2.7/README.md"
outputFile = "output_wordcount_python"
sc = SparkContext("local", "WordCount Python Application")
input = sc.textFile(inputFile)
words = input.flatMap(lambda line: line.split())
counts = words.map(lambda word: (word, 1)).reduceByKey(lambda a, b: a + b)
counts.saveAsTextFile(outputFile)
sc.stop()
