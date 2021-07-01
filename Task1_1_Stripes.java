package edu.rmit.cosc2367.s3779009.Assignment2_Task1;

import java.io.IOException;
import java.util.Set;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;


public class Task1_1_Stripes {
private static final Logger LOG = Logger.getLogger(Task1_1_Stripes.class);
	
	public static class TokenizerMapper extends Mapper <Object, Text, Text, MapWritable> {
		private MapWritable occmap = new MapWritable();
		private Text word = new Text();
		
		public void map(Object key, Text value, Context context) throws IOException, InterruptedException 
		{
			LOG.info("The Mapper Task of Pranamya K, S3779009 of Task1_1_Stripes Assignment 2");
			String[] tokens = value.toString().split("\\s+");
			for(String token : tokens) {
				token.toLowerCase();
			}
			int neighbors = context.getConfiguration().getInt("neighbors", tokens.length);
			
			if(tokens.length > 1) {
				for(int i =0 ; i < tokens.length; i++) {
					tokens[i].replaceAll("\\W+", "");
					word.set(tokens[i]);
					occmap.clear();
					
					int start = (i - neighbors < 0) ? 0 : i - neighbors;
					int end = (i + neighbors >= tokens.length) ? tokens.length - 1 : i + neighbors;
					for(int j = start; j <= end ; j++) {
						if (j==i) continue;
						Text neighbor = new Text(tokens[j].replaceAll("\\W+", ""));
						if(occmap.containsKey(neighbor)) {
							IntWritable count = (IntWritable)occmap.get(neighbor);
							count.set(count.get()+1);
						}
						else {
							occmap.put(neighbor, new IntWritable(1));
						}
					}
					context.write(word, occmap);
				}
			}
		}
		
	}
	public static class IntSumReducer extends Reducer<Text,MapWritable,Text,MapWritable> {
		private MapWritable result = new MapWritable();
		
		//Reducer function to reduce and generate output.
		public void reduce(Text key, Iterable<MapWritable> values,	Context context) throws IOException, InterruptedException
		{
			LOG.info("The Reducer Task of Pranamya K, S3779009 of Task1_1_Stripes Assignment 2");
			result.clear();
			for (MapWritable value : values) {
	            addAll(value);
	        }
	        context.write(key, result);
	        
	        
		}
		private void addAll(MapWritable value1) {
			// TODO Auto-generated method stub
			 Set<Writable> keys = value1.keySet();
		        for (Writable key : keys) {
		            IntWritable fromCount = (IntWritable) value1.get(key);
		            if (result.containsKey(key)) {
		                IntWritable count = (IntWritable) result.get(key);
		                count.set(count.get() + fromCount.get());
		                //result.put(key, count);
		            } else {
		                result.put(key, fromCount);
		            }
		        }
		}
	}

	public static void main(String[] args) throws Exception {
		// TODO Auto-generated method stub
		LOG.setLevel(Level.DEBUG);
		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf, "Stripes Abs Freq");
		job.setJarByClass(Task1_1_Stripes.class);
		job.setMapperClass(TokenizerMapper.class);
		job.setCombinerClass(IntSumReducer.class);
		job.setReducerClass(IntSumReducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(MapWritable.class);
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}


}
