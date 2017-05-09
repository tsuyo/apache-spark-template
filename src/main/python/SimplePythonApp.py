"""SimplePythonApp.py"""
from pyspark import SparkContext

logFile = "/opt/spark-2.1.1-bin-hadoop2.7/README.md"
sc = SparkContext("local", "Simple Python Application")
logData = sc.textFile(logFile).cache()

numAs = logData.filter(lambda s: 'a' in s).count()
numBs = logData.filter(lambda s: 'b' in s).count()

print("Lines with a: %i, Lines with b: %i" % (numAs, numBs))

sc.stop()
