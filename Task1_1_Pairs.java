package edu.rmit.cosc2367.s3779009.Assignment2_Task1;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;


public class Task1_1_Pairs {
	private static final Logger LOG = Logger.getLogger(Task1_1_Pairs.class);
	
	public static class TokenizerMapper extends Mapper <Object, Text, WordPair, IntWritable> {
		private WordPair wordpair = new WordPair(); 
		private IntWritable ONE = new IntWritable(1);
		
		
		// Mapper Function for task 1_1 with absolute frequency maintenance
		
		public void map(Object key, Text value, Context context) throws IOException, InterruptedException 
		{
			LOG.info("The Mapper Task of Pranamya K, S3779009 of Task1_1_Pairs Assignment 2");
			String[] tokens = value.toString().split("\\s+");
			for(String token : tokens) {
				token.toLowerCase();
			}
			int neighbors = context.getConfiguration().getInt("neighbors", tokens.length);
			if(tokens.length > 1) {
				for(int i =0 ; i < tokens.length; i++) {
					tokens[i].replaceAll("\\W+", "");
					wordpair.setWord(tokens[i]);
					int start = (i - neighbors < 0) ? 0 : i - neighbors;
					int end = (i + neighbors >= tokens.length) ? tokens.length - 1 : i + neighbors;
					for(int j = start; j <= end ; j++) {
						if (j==i) continue;
						wordpair.setNeighbour(tokens[j].replaceAll("\\W+", ""));
						context.write(wordpair, ONE);		 
					}
				}
			}
					 
		}
		
	}
	public static class IntSumReducer extends Reducer<WordPair,IntWritable,WordPair,IntWritable> {
		private IntWritable result = new IntWritable();
		
		//Reducer function to reduce and generate output.
		public void reduce(WordPair key, Iterable<IntWritable> values,	Context context) throws IOException, InterruptedException
		{
			LOG.info("The Reducer Task of Pranamya K, S3779009 of Task1_1_Pairs Assignment 2");
			int sum = 0;
			for (IntWritable val : values) {
				sum += val.get();
			}
			result.set(sum);
			context.write(key, result);
		}
	}

	public static void main(String[] args) throws Exception {
		// TODO Auto-generated method stub
		LOG.setLevel(Level.DEBUG);
		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf, "Pairs Abs Freq.");
		job.setJarByClass(Task1_1_Pairs.class);
		job.setMapperClass(TokenizerMapper.class);
		job.setCombinerClass(IntSumReducer.class);
		job.setReducerClass(IntSumReducer.class);
		job.setOutputKeyClass(WordPair.class);
		job.setOutputValueClass(IntWritable.class);
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}


}
