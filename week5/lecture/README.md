### Python Week 5 Lecture Resources

This folder contains the Python code (*place_filter_mapper.py*, *replicate_join_mapper.py*) and execution script (*job_chain_driver.sh*) of a two job MapReduce program. Both jobs are map-only jobs, no reducer script is provided. The program takes up to four arguments: two input files, one output path and an optional country name. 

The first input file contains place information with the following format:

```
place_id \t woeid \t lat \t longi \t place_name \t place_url
```

You have used a file of this format (*place.txt*) in week 4 tutorial.

The second input file contains photo information with the following format:

```
photo_id \t owner \t tags \t date_taken \t place_id \t accuracy
```

You have used a file of this format (*partial.txt*) in week 4 tutorial.

The program first extracts all rows from the place input file containing a given country name in place_url field. If no coutry code is given, the default one "Australia" will be used. This step is implemented as a map-only job (*place_filter_maper.py*). The output of this job is a list of key-value pairs of the format 

```
place_id  "\t"  place_url
```

The output is writted to a temporary output location: "[output_location]/tmpFile". It is then copied localy with the command: 
```
hdfs dfs -copyToLocal ""$3"tmpFile/part-00000"
```

The second job (*replicate_join_mapper.py*) is also a map-only job. It joins the output of the first job with the photo input file on field *place_id* and output a list of record of the following format:
```
photo_id \t date_taken \t place_url
```

The mapper extracts place_id and place_url mappings from the output of the first job and save it as a dictionary. This is used to match records in the photo input file. 


It showcase the following features:

* Share simple variable among all mappers as script input. e.g.
	```
	mapper "place_filter_mapper.py $countries"
	```
* Share small file among all mappers using *-file(s)* option. e.g.
	```
	-files part-00000
	```
* Indicate map-only job by setting reducer number to 0. e.g.
	```
	-D mapreduce.job.reduces=0
	```
* chain and automatically execute multiple jobs by calling streaming API multiple times.


Detailed explanation of hadoop streaming options used in *job_chain_driver.sh* can be found in offical streaming documentation: [Hadoop Streaming](http://hadoop.apache.org/docs/current/hadoop-streaming/HadoopStreaming.html)

[**Acknowledgement**] The python code and execution script is provided by Andrian Yang 
