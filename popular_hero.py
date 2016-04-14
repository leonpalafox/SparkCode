#most popular superhero
#hero id coocurrences, coocurrences
from pyspark import SparkConf, SparkContext
conf = SparkConf().setMaster("local").setAppName("PopularMovie")
sc = SparkContext(conf = conf)

def countCoOccurences(line):
    elements = line.split()
    return (int(elements[0]), len(elements) -1)

def parseNames(line):
   fields = line.split('\"')
   return (int(fields[0]), fields[1].encode("utf8"))
   
names = sc.textFile('file:///C:/Users/leon/Dropbox/Code/Python/SparkCode/Marvel-Names.txt')
namesRdd = names.map(parseNames) #instead using a broadcast
lines = sc.textFile('file:///C:/Users/leon/Dropbox/Code/Python/SparkCode/Marvel-Graph.txt')    

pairings = lines.map(countCoOccurences)
totalFriendsByCharacter = pairings.reduceByKey(lambda x,y: x+y)
flipped = totalFriendsByCharacter.map(lambda (x,y):(y,x))

mostPopular = flipped.min()
mostPopularName = namesRdd.lookup(mostPopular[1])[0]
print mostPopularName + ' is the most popular super hero, with '+ str(mostPopular[0]) + ' co-appearances'