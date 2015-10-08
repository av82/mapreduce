from mrjob.job import MRJob

class MRRatingCounter(MRJob):
    def mapper(self,key,line):
        (userID,movieID,rating,timestamp)=line.split('\t')
        yield rating,1 #how many 1-ratings, 2-ratings etc
    
    def reducer(self,rating,occurences):
        yield rating, sum(occurences)

if __name__=='__main__':
    MRRatingCounter.run()