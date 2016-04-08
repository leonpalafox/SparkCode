#cd \Users\leon\Dropbox\Code\Python\SparkCode
#spark-submit ratings_counter.py

from pyspark import SparkConf, SparkContext
import collections

conf = SparkConf().setMaster("local").setAppName("RatingsHistogram")
sc = SparkContext(conf = conf)

lines = sc.textFile("C:\Users\leon\Documents\Data\HadoopData\movie_lense\u.data")
ratings = lines.map(lambda x: x.split()[2])
result = ratings.countByValue()

sortedResults = collections.OrderedDict(sorted(result.items()))
for key, value in sortedResults.iteritems():
    print "%s %i"% (key, value)