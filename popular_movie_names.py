#We need broadcast variables, like in MapReduce
#Sparks allows us to send information once
#Broadcast variables
#sc.broadcast() <- does the magic
#.value() <- retrieves the object from the broadcast
#find the most popular movie
#cd \Users\leon\Dropbox\Code\Python\SparkCode
#spark-submit popular_movie.py
#userID, MOVIE ID, Rating, TIMESTAMP
from pyspark import SparkConf, SparkContext

def loadMovieNames(): #reates the dictionary with movie names
    movieNames = {}
    with open('C:\Users\leon\Documents\Data\HadoopData\movie_lense\u.ITEM') as f:
        for line in f:
            fields = line.split('|')
            movieNames[int(fields[0])] = fields[1]
    return movieNames

conf = SparkConf().setMaster("local").setAppName("PopularMovie")
sc = SparkContext(conf = conf)

nameDict = sc.broadcast(loadMovieNames())


lines = sc.textFile('C:\Users\leon\Documents\Data\HadoopData\movie_lense\u.data')
movies = lines.map(lambda x: (int(x.split()[1]), 1))
movieCounts = movies.reduceByKey(lambda x, y: x+y) #reduce by key is just the reduction operation to match movies with the same key
flipped = movieCounts.map(lambda (x,y):(y,x))
sortedMovies = flipped.sortByKey()
sortedMoviesWithNames = sortedMovies.map(lambda (count, movie) : (nameDict.value[movie], count))
results = sortedMoviesWithNames.collect()
for result in results:
    print result