from pyspark.sql import SparkSession
from pyspark.sql import Row
from pyspark.sql import functions

def loadMovieNames():
    movieNames = {}
    with open("ml-100k/u.item") as file:
        for line in file:
            fields = line.split("|")
            movieNames[int(fields[0])] = fields[1]
    return movieNames

def parseInput(line):
    fields = line.split("\t")
    return Row(movieID=int(fields[1]), rating=float(fields[2]))

if __name__ == "__main__":
    spark = SparkSession.builder.appName("PopularMovies").getOrCreate()
    movieNames = loadMovieNames()
    lines = spark.sparkContext.textFile("hdfs:///user/maria_dev/ml-100k/u.data")
    movies = lines.map(parseInput)
    movieDataSet = spark.createDataFrame(movies)
    averageRatings = movieDataSet.groupBy("movieID").avg("rating")
    counts = movieDataSet.groupBy("movieID").count()
    averagesAndCounts = averageRatings.join(counts, "movieID")
    topTen = averagesAndCounts.orderBy("avg(rating)").take(10)
    for movie in topTen:
        print(movieNames[movie[0]], movie[1], movie[2])
    spark.stop()
