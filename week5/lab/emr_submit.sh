#!/bin/bash
mkdir ~/data
aws s3 cp s3://comp5349-2019/lab-data/week5/ratings.csv ~/data
aws s3 cp s3://comp5349-2019/lab-data/week5/movies.csv ~/data

spark-submit \
    --master local[4] \
    AverageRatingPerGenre.py \
    --input file:///home/hadoop/data/ \
    --output file:///home/hadoop/rating_out/
	 

    
