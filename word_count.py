#This counts the words in a book
from pyspark import SparkConf, SparkContext
import collections

conf = SparkConf().setMaster("local").setAppName("MinTemperatures")
sc = SparkContext(conf = conf)

input = sc.textFile('file:///C:/Users/leon/Dropbox/Code/Python/SparkCode/book.txt')
words = input.flatMap(lambda x: x.split())
wordCounts = words.countByValue()

for result, count in wordCounts.items():
    rel = result.encode('ascii', 'ignore')    
    print rel, count