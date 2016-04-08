#this groups friends by age
#cd \Users\leon\Dropbox\Code\Python\SparkCode
#spark-submit friends_by_age.py
from pyspark import SparkConf, SparkContext

conf =SparkConf().setMaster("local").setAppName("FriendsByAge")
sc = SparkContext(conf = conf) #spark context object

def parseLine(line):
    fields = line.split(',')
    age = int(fields[2])
    numFriends = int(fields[3])
    return (age, numFriends)

lines = sc.textFile(r'C:\Users\leon\Dropbox\Code\Python\SparkCode\fakefriends.csv')
rdd = lines.map(parseLine) #transforms input data
totalsByAge = rdd.mapValues(lambda x: (x,1))
#.reduceByKey(lambda x, y: (x[0]+y[0], x[1]+y[1]))
averagesByAge = totalsByAge.mapValues(lambda x:x[0]/x[1])
results = averagesByAge.collect()
results = totalsByAge.collect()
for results in results:
    print results    