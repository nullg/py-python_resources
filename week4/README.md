### Python Week 4 Resources

This folder contains two implementations of counting tags used by users task:

* __native__: this folder contains an naive implementation, containing only the map and reduce phase. *tag_driver.sh* is a simple script provided as an example of using streaming API. It takes two arguments: the *input file* and the *output path*. Below is an example:

	```
	tag_driver.sh partial.txt out
	```

* __combiner__: this folder contains the version using combiner. You will notice that the mapper implementation (*tag_mapper.py*) is quite similar, the only difference is the output format. In the naive version, we have 

	```python
	print("{}\t{}".format(tag, username))
	```
	and in the combiner version, we have 

	```python	
	print("{}\t{}=1".format(tag, username))
	``` 

	The reducer implementations also reflect the changed input value part.  We also provide a script *tag_driver_combiner.sh* showing you how to indicate combiner with option *-combiner* followed by the actual combiner script, which is also the reducer script *tag_reducer.py*. The usage of *tag_driver_combiner.sh* is exactly the same as *tag_driver.sh*. Below is an example:

	```
	tag_combiner_driver.sh partial.txt out_combiner
	```

To find out more about various options for use in hadoop streaming, see [Hadoop Streaming](http://hadoop.apache.org/docs/current/hadoop-streaming/HadoopStreaming.html)

