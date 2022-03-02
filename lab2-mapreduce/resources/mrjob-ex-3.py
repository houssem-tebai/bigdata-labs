from mrjob.job import MRJob
from mrjob.step import MRStep

class RatingCount(MRJob):
    def steps(self):
        return [
            MRStep(mapper=self.mapper_get_ratings,
                   reducer=self.reducer_count_ratings),
            MRStep(reducer=self.reducer_rank)
        ]

    def mapper_get_ratings(self, _, line):
        (userID, movieID, ratings, time) = line.split(',')   
        yield movieID, 1

    def reducer_count_ratings(self, key, values):
        yield str(sum(values)).zfill(5), key
        
    def reducer_rank (self, count, movies):
        for movie in movies:
            yield movie, count

if __name__ == '__main__':
    RatingCount.run()