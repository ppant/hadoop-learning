
 /*******************************************************************************
    Copyright (c) 2014 [www.pradeeppant.com]
  ******************************************************************************/

package com.pradeeppant.mapreduce;

import java.io.IOException;
import java.util.Iterator;
import java.util.StringTokenizer;

/*
 * All org.apache.hadoop packages can be imported using the jar present in lib 
 * directory of this java project.
 */

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.TextInputFormat;
import org.apache.hadoop.mapred.TextOutputFormat;


/**
 * @author ppant
 * @version 1.0
 * @since 10-NOV-2014
 * @package com.pradeeppant.mapreduce
 * @copyright Copyright (c) 2014  [www.pradeeppant.com]
 * <p>The CountingWord program counts the number of times a word has appeared in the input file we give.
 * We write a map reduce code to achieve this, where mapper makes key value pair from the input file
 * and reducer does aggregation on this key value pair.
 */

public class CountingWord {

	/** 
	 * @author ppant
	 * @interface Mapper
	 * <p>Map class is static and extends MapReduceBase and implements Mapper 
	 * interface having four hadoop generics type LongWritable, Text, Text, 
	 * IntWritable.
	 */
	
	public static class Map extends MapReduceBase implements
			Mapper<LongWritable, Text, Text, IntWritable> {

	//Mapper
		
		
/**
 * @method map
 * <p>This method takes the input as text data type and splits the input into words
 * and makes key value pair. This key value pair is passed to reducer.                                             
 * @method_arguments key, value, output, reporter
 * @return void
 */	

		/*
		 * (non-Javadoc)
		 * @see org.apache.hadoop.mapred.Mapper#map(java.lang.Object, java.lang.Object, org.apache.hadoop.mapred.OutputCollector, org.apache.hadoop.mapred.Reporter)
		 */
		
		
		@Override
		public void map(LongWritable key, Text value, OutputCollector<Text, IntWritable> output, Reporter reporter)
				throws IOException {

			//Converting the record (single line) to String and storing it in a String variable line
			String line = value.toString();

			//StringTokenizer is breaking the record (line) into words
			StringTokenizer tokenizer = new StringTokenizer(line);

			//Iterating through all the words available in that line and forming the key value pair	
			while (tokenizer.hasMoreTokens()) {
				value.set(tokenizer.nextToken());
				
				//Sending to output collector which inturn passes the same to reducer
				output.collect(value, new IntWritable(1));

			}
							
		}
	}

  //Reducer
	
/** 
 * @author ppant
 * @interface Reducer
 * <p>Reduce class is static and extends MapReduceBase and implements Reducer 
 * interface having four hadoop generics type Text, IntWritable, Text, IntWritable.
 */
	
	public static class Reduce extends MapReduceBase implements
			Reducer<Text, IntWritable, Text, IntWritable> {
	

/**
 * @method reduce
 * <p>This method takes the input as key and list of values pair from mapper, it does aggregation
 * based on keys and produces the final output.                                               
 * @method_arguments key, values, output, reporter	
 * @return void
 */	
		
		/* (non-Javadoc)
		 * @see org.apache.hadoop.mapred.Reducer#reduce(java.lang.Object, java.util.Iterator, org.apache.hadoop.mapred.OutputCollector, org.apache.hadoop.mapred.Reporter)
		 */
		
		@Override
		
		public void reduce(Text key, Iterator<IntWritable> values,
				OutputCollector<Text, IntWritable> output, Reporter reporter)
				throws IOException {

          //Defining a local variable sum of type int
			int sum = 0;

		/*
		 * Iterates through all the values available with a key and adds them together 
		 * and give the final result as the key and sum of its values.
		 */

			while (values.hasNext()) {

				sum += values.next().get();
				
				
			}
			
			//Dumping the output
			output.collect(key, new IntWritable(sum));
			
		}
	}


  //Driver

/**
 * @method main
 * <p>This method is used for setting all the configuration properties.
 * It acts as a driver for map reduce code.
 * @return void
 * @method_arguments args
 * @throws Exception
 */
	
	public static void main(String[] args) throws Exception {

		//Creating a JobConf object and assigning a job name for identification purposes
		JobConf conf = new JobConf(CountingWord.class);
		conf.setJobName("countingword");

		//Setting configuration object with the Data Type of output Key and Value
		conf.setOutputKeyClass(Text.class);
		conf.setOutputValueClass(IntWritable.class);

		//Providing the mapper and reducer class names
		conf.setMapperClass(Map.class);
		conf.setReducerClass(Reduce.class);

		//Setting format of input and output
		conf.setInputFormat(TextInputFormat.class);
		conf.setOutputFormat(TextOutputFormat.class);

		//The hdfs input and output directory to be fetched from the command line
		FileInputFormat.setInputPaths(conf, new Path(args[0]));
		FileOutputFormat.setOutputPath(conf, new Path(args[1]));

		//Running the job
		JobClient.runJob(conf);

	}
}


