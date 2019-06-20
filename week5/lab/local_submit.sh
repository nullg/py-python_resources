#!/bin/bash

spark-submit \
    --master local[2] \
    AverageRatingPerGenre.py
    
