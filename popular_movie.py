#find the most popular movie
#cd \Users\leon\Dropbox\Code\Python\SparkCode
#spark-submit popular_movie.py
#userID, MOVIE ID, Rating, TIMESTAMP
from pyspark import SparkConf, SparkContext
conf = SparkConf().setMaster("local").setAppName("PopularMovie")
sc = SparkContext(conf = conf)

lines = sc.textFile('C:\Users\leon\Documents\Data\HadoopData\movie_lense\u.data')
movies = lines.map(lambda x: (int(x.split()[1]), 1))
movieCounts = movies.reduceByKey(lambda x, y: x+y) #reduce by key is just the reduction operation to match movies with the same key
flipped = movieCounts.map(lambda (x,y):(y,x))
sortedMovies = flipped.sortByKey()
results = sortedMovies.collect()
for result in results:
    print result