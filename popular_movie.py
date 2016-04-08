#find the most popular movie
#cd \Users\leon\Dropbox\Code\Python\SparkCode
#spark-submit popular_movie.py

from pyspark import SparkConf, SparkContext
conf = SparkConf().setMaster("local").setAppName("PopularMovie")
sc = SparkContext(conf = conf)

