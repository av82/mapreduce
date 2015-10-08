from mrjob.job import MRJob
import numpy

class AverageMovieRating(MRJob):
    #taming-big-data-with-mapreduce-and-hadoop
    def mapper(self,key,line):
        (userID,movieID,rating,timestamp)=line.split('\t')
        yield movieID,float(rating) #to reduce movieid by rating to compute average
    
    def reducer(self,movieID,rating):
       total=0
       numElements=0
       for x in rating:
           total += x
           numElements += 1
       yield movieID, (total/numElements)
        

if __name__=='__main__':
    AverageMovieRating.run()

    
