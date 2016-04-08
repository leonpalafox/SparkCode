#This counts the words in a book
from pyspark import SparkConf, SparkContext
import collections
import re

def normalizeWords(text):
    return re.compile(r'\W+', re.UNICODE).split(text.lower())

conf = SparkConf().setMaster("local").setAppName("MinTemperatures")
sc = SparkContext(conf = conf)

input = sc.textFile('file:///C:/Users/leon/Dropbox/Code/Python/SparkCode/book.txt')
words = input.flatMap(normalizeWords)
words = words.map(lambda x: (x,len(x))).filter(lambda t:t[1]>=6).map(lambda y:y[0])
wordCounts = words.map(lambda x:(x,1)).reduceByKey(lambda x, y: x+y)

#.map(lambda(x:x[0]))
wordCountsSorted = wordCounts.map(lambda (x,y): (y,x)).sortByKey()
results = wordCountsSorted.collect()
#results = sorted(wordCounts.collect(), key=lambda key: key[1])
for count, rel in results:
    count1 = str(count)
    rel1 =  rel.encode('ascii', 'ignore')
    if (rel1):
        print rel1 + ':\t\t' + count1    

#for result, count in wordCounts.items():
#    rel = result.encode('ascii', 'ignore')    
#    print rel, count