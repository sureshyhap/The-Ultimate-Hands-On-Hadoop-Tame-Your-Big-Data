from mrjob.job import MRJob
from mrjob.step import MRStep

class RatingsCount(MRJob):
    def steps(self):
        return [
            MRStep(mapper=self.mapper, reducer=self.reducer),
            MRStep(reducer=self.reducer2)
        ]

    def mapper(self, _, line):
        (userID, movieID, rating, timestamp) = line.split("\t")
        yield movieID, 1

    def reducer(self, movieID, values):
        yield str(sum(values)).zfill(5), movieID

    def reducer2(self, numRatings, movieIDs):
        for movieID in movieIDs:
            yield movieID, int(numRatings.lstrip("0"))


if __name__ == "__main__":
    RatingsCount.run()
