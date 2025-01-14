from pyspark import SparkConf, SparkContext

def loadMovieNames():
    movieNames = {}
    with open("ml-100k/u.item") as file:
        for line in file:
            tokens = line.split("|")
            movieNames[int(tokens[0])] = tokens[1]
    return movieNames

def parseInput(line):
    tokens = line.split("\t")
    return (int(tokens[1]), (float(tokens[2]), 1.0))

if __name__ == "__main__":
    conf = SparkConf().setAppName("WorstMovies")
    sc = SparkContext(conf=conf)

    movieNames = loadMovieNames()
    lines = sc.textFile("hdfs:///user/maria_dev/ml-100k/u.data")
    movieRatings = lines.map(parseInput)
    ratingTotalsAndCount = movieRatings.reduceByKey(lambda rating1, rating2: (rating1[0] + rating2[0], rating1[1] + rating2[1]))
    averageRatings = ratingTotalsAndCount.mapValues(lambda totalAndCount: totalAndCount[0] / totalAndCount[1])
    sortedMovies = averageRatings.sortBy(lambda x: x[1])
    results = sortedMovies.take(10)
    for result in results:
        print(movieNames[result[0]], result[1])
