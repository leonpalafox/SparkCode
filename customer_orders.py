#example on customer oreders
#cd \Users\leon\Dropbox\Code\Python\SparkCode
#spark-submit friends_by_age.py
from pyspark import SparkConf, SparkContext
import collections
import re
conf =SparkConf().setMaster("local").setAppName("FriendsByAge")
sc = SparkContext(conf = conf) #spark context object

def parseLine(line):
    fields = line.split(',')
    customerID = int(fields[0])
    spent = float(fields[2])
    return (customerID, spent)
    
input = sc.textFile('file:///C:/Users/leon/Dropbox/Code/Python/SparkCode/customer-orders.csv')
parsedLines = input.map(parseLine)
rdd=parsedLines.reduceByKey(lambda x,y: x+y)
inverted_rdd = rdd.map(lambda (x,y):(y,x)).sortByKey(ascending = False)
results = inverted_rdd.collect()
for results in results:
    print results  
