package edu.rmit.cosc2367.s3779009.Assignment2_Task1;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Reducer.Context;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;

public class Task1_2_Pairs {
private static final Logger LOG = Logger.getLogger(Task1_2_Pairs.class);
	
	// Mapper Class which counts the pair with whole line as a neighbour context. Also send one (wordpair, *) at the end
	// of each upper iteration.
	public static class TokenizerMapper extends Mapper <Object, Text, WordPair, DoubleWritable> {
		private WordPair wordpair = new WordPair(); 
		private DoubleWritable ONE = new DoubleWritable(1);
		private DoubleWritable totalcount = new DoubleWritable();		
		
		// Mapper Function for task 1_2 with relative frequency maintenance
		public void map(Object key, Text value, Context context) throws IOException, InterruptedException 
		{
			LOG.info("The Mapper Task of Pranamya K, S3779009 of Task1_2_Pairs Assignment 1");
			String[] tokens = value.toString().split("\\s+");
			int neighbors = context.getConfiguration().getInt("neighbors", tokens.length);
			if(tokens.length > 1) {
				for(int i =0 ; i < tokens.length; i++) {
					tokens[i].replaceAll("\\W+", "");
					if(tokens[i].equals("")|| tokens[i].isEmpty()) {
						continue;
					}
					wordpair.setWord(tokens[i]);
					int start = (i - neighbors < 0) ? 0 : i - neighbors;
					int end = (i + neighbors >= tokens.length) ? tokens.length - 1 : i + neighbors;
					for(int j = start; j <= end ; j++) {
						if (j==i) continue;
						if (tokens[j].isEmpty() || tokens[i].equals("")) continue;
						wordpair.setNeighbour(tokens[j].replaceAll("\\W+", ""));
						context.write(wordpair, ONE);		 
					}	
					wordpair.setNeighbour("*");
					totalcount.set(end-start);
					context.write(wordpair, totalcount);										
				}
			}
					 
		}
		
	}
	

	
	public static class TokenizerCombiner extends Reducer<WordPair,DoubleWritable,WordPair,DoubleWritable>{
		private DoubleWritable result = new DoubleWritable();
		//Combiner function to combine and generate output.
		public void reduce(WordPair key, Iterable<DoubleWritable> values,	Context context) throws IOException, InterruptedException
		{
			LOG.info("The Combiner Task of Pranamya K, S3779009 of Task1_2_Pairs Assignment 1");
			double sum = 0;
			for (DoubleWritable val : values) {
				sum += (double) val.get();
			}
			result.set(sum);
			context.write(key, result);
		}
	}
	
	// Custom Partitioner class to handle special * character for order inversion so that we can send the words to reducer
	// on the basis of left word only.
	public static class WordPairPartitioner extends Partitioner<WordPair, DoubleWritable>{
		@Override
		public int getPartition(WordPair key, DoubleWritable value, int numPartitions) {
			// TODO Auto-generated method stub
			return (key.getWord().hashCode() & Integer.MAX_VALUE) % numPartitions ;
		}		
	}
	
	// Reducer class that first calculates total count of the WordPair and then calculate relative frequency.
	public static class IntSumReducer extends Reducer<WordPair,DoubleWritable,WordPair,DoubleWritable> {
		
		private DoubleWritable totalcount = new DoubleWritable();
		private DoubleWritable relativeCount = new DoubleWritable();
		private Text current = new Text("Blank");
		private Text flag1 = new Text("*");
		//Reducer function to reduce and generate output.
		public void reduce(WordPair key, Iterable<DoubleWritable> values, Context context) throws IOException, InterruptedException
		{
			LOG.info("The Reducer Task of Pranamya K, S3779009 of Task1_2_Pairs Assignment 2");
			if(key.getNeighbour().equals(flag1)) {
				if(key.getWord().equals(current)) {
					totalcount.set(totalcount.get() + getTotal(values));
				}
				else {
					current.set(key.getWord());
					totalcount.set(0);
					totalcount.set(getTotal(values));					
				}
			}
			else {
				double count = getTotal(values);
				relativeCount.set(count/(double)totalcount.get());
				context.write(key, relativeCount);
			}
			
		}
		private double getTotal(Iterable<DoubleWritable> values) {
			// TODO Auto-generated method stub
			double count = 0;
			for (DoubleWritable value : values) {
				count = count + (double) value.get();
			}
			return count;
		}			
	}

	public static void main(String[] args) throws Exception {
		// TODO Auto-generated method stub
		LOG.setLevel(Level.DEBUG);
		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf, "Pairs Relative freq.");
		job.setJarByClass(Task1_2_Pairs.class);
		job.setMapperClass(TokenizerMapper.class);
		job.setCombinerClass(TokenizerCombiner.class);
		job.setPartitionerClass(WordPairPartitioner.class);
		job.setNumReduceTasks(3);
		job.setReducerClass(IntSumReducer.class);
	
		job.setOutputKeyClass(WordPair.class);
		job.setOutputValueClass(DoubleWritable.class);
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
}
