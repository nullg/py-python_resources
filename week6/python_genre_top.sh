export PYSPARK_PYTHON=python3
spark-submit \
    --master yarn \
    --deploy-mode cluster \
    --num-executors 3 \
    Top5MoviesPerGenreNaive.py \
    --input movies/ \
    --output genre-top-python/
