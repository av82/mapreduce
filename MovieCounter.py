from mrjob.job import MRJob

class UserMovieCounter(MRJob):
    def mapper(self,key,line):
        (userID,movieID,rating,timestamp)=line.split('\t')
        yield userID,1 #how many movies user has watched
            
    def reducer(self,userID,occurences):
        yield userID,sum(occurences)
    
    
if __name__=='__main__':
    UserMovieCounter.run()
            
        