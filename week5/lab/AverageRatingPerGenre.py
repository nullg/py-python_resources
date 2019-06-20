# Calculate the average rating of each genre
# In order to run this, we use spark-submit, below is the 
# spark-submit  \
#   --master local[2] \
    #   AverageRatingPerGenre.py
#   --input input-path
#   --output outputfile

from pyspark import SparkContext
import argparse
import csv


"""
This module includes a few functions used in computing average rating per genre
"""
def pairMovieToGenre(record):
    """This function converts entries of movies.csv into key,value pair of the following format
    (movieID, genre)
    since there may be multiple genre per movie, this function returns a list of tuples
    Args:
        record (str): A row of CSV file, with three columns separated by comma
    Returns:
        The return value is a list of tuples, each tuple contains (movieID, genre)
    """
    for row in csv.reader([record]):
        if len(row) != 3:
            continue
        movieID, genreList = row[0],row[2]
        return [(movieID, genre) for genre in genreList.split("|")]

def extractRating(record):
    """ This function converts entries of ratings.csv into key,value pair of the following format
    (movieID, rating)
    Args:
        record (str): A row of CSV file, with four columns separated by comma
    Returns:
        The return value is a tuple (movieID, genre)
    """
    try:
        userID, movieID, rating, timestamp = record.split(",")
        rating = float(rating)
        return (movieID, rating)
    except:
        return ()

def mapToPair(line):
    """ This function converts tuples of (genre, rating) into key,value pair of the following format
    (genre,rating)
    
    Args:
        line (str): A touple of  (genre, rating) 
    Returns:
        The return value is a tuple  (genre, rating) 
    """
    genre, rating = line
    return (genre, rating)


def mergeRating(accumulatedPair, currentRating):
    """This funtion update a current  summary (ratingTotal, ratingCount) with a new rating value.
    
    Args:
        accumulatedPair (tuple): a tuple of (ratingTotal, ratingCount)
        currentRating (float):a new rating value, 
    Returns:
        The return value is an updated tuple of (ratingTotal, ratingCount)
    
    """
    ratingTotal, ratingCount = accumulatedPair
    ratingTotal += currentRating
    ratingCount += 1
    return (ratingTotal, ratingCount)


def mergeCombiners(accumulatedPair1, accumulatedPair2):
    """This function merges two intermedate summaries of the format (ratingTotal, ratingCount)
  
    Args:
        accumulatedPair1 (tuple): a tuple of (ratingTotal, ratingCount)
        accumulatedPair2 (fuple): a tuple of (ratingTotal, ratingCount) 
    Returns:
        The return value is an updated tuple of (ratingTotal, ratingCount)
    """
    ratingTotal1, ratingCount1 = accumulatedPair1
    ratingTotal2, ratingCount2 = accumulatedPair2
    return (ratingTotal1+ratingTotal2, ratingCount1+ratingCount2)


def mapAverageRating(line):
    """This function compute the average with a given sum and count for a genre
    Args:
        line (tuple): a tuple of (genre, (ratingTotal,ratingCount))
    Returns:
        The return value is a tuple of (genre, average_rating)
    """

    genre, ratingTotalCount = line
    ratingAverage = ratingTotalCount[0]/ratingTotalCount[1]
    return (genre, ratingAverage)



if __name__ == "__main__":
    sc = SparkContext(appName="Average Rating per Genre")
    parser = argparse.ArgumentParser()
    parser.add_argument("--input", help="the input path",
                        default='~/comp5349/lab_commons/week5/')
    parser.add_argument("--output", help="the output path", 
                        default='rating_out_script') 
    args = parser.parse_args()
    input_path = args.input
    output_path = args.output
    ratings = sc.textFile(input_path + "ratings.csv")
    movieData = sc.textFile(input_path + "movies.csv")
    movieRatings = ratings.map(extractRating)
    movieGenre = movieData.flatMap(pairMovieToGenre) # we use flatMap as there are multiple genre per movie
    genreRatings = movieGenre.join(movieRatings).values()
    genreRatingsAverage = genreRatings.aggregateByKey((0.0,0), mergeRating, mergeCombiners, 1).map(mapAverageRating)
    genreRatingsAverage.saveAsTextFile(output_path)
