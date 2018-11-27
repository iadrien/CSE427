import sys
import re
from pyspark import SparkContext
from pyspark import SparkConf
export PYSPARK_SUBMIT_ARGS="--master local[2] pyspark-shell"

sc = SparkContext()
# read from local filesystem
data = sc.textFile("file:/home/cloudera/training_materials/dev1/data/devicestatus.txt")

d = data.map(lambda l: l.split(',')).filter(lambda l : len(l) == 14).map(lambda l : (l[12], l[13], l[0], l[1].split(' ')[0], l[1].split(' ')[1])).filter(lambda l : l[0] != '0' and l[1] != '0').map(lambda l : l[0] + "," +  l[1] + "," + l[2] + "," + l[3] + "," + l[4])

d.saveAsTextFile("hdfs:///loudacre/devicestatus_etl")
sc.stop()
