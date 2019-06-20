export PYSPARK_PYTHON=python3
spark-submit \
    --master yarn \
    --deploy-mode cluster \
    --num-executors 3 \
    --py-files ml_utils.py AverageRatingPerGenre.py \
    --input movies/ \
    --output genre-avg-python/
