package org.samples.mapreduce.training;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.NLineInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

public class InputFormatSamples {

	public static void main(String[] args) throws Exception {
		if (args.length != 2) {
			System.err.println("Insufficient args");
			System.exit(-1);
		}
//		testTextInputFileFormat(args[0],args[1]);
		testSequenceTextInputFileFormat(args[0],args[1]);
		testNLineOutput(args[0],args[1]);
	}

	
	public static void testTextInputFileFormat(String input, String output) throws Exception {
		Configuration conf = new Configuration();
		conf.set("mapred.job.tracker", "hdfs://localhost:50001");
		Job job = new Job(conf, "Drug Amount Spent");
		job.setJarByClass(InputFormatSamples.class); 
		job.setNumReduceTasks(1);
		job.setInputFormatClass(TextInputFormat.class); 
		job.setOutputFormatClass(TextOutputFormat.class);
		FileInputFormat.addInputPath(job, new Path(input));
		FileOutputFormat.setOutputPath(job, new Path(output));
		job.waitForCompletion(true);
	}
	
	public static void testSequenceTextInputFileFormat(String input, String output) throws Exception {
		Configuration conf = new Configuration();
		conf.set("mapred.job.tracker", "hdfs://localhost:50001");
		Job job = new Job(conf, "Drug Amount Spent");
		job.setJarByClass(InputFormatSamples.class); 
		job.setNumReduceTasks(1);	
		job.setInputFormatClass(TextInputFormat.class); 
		job.setOutputFormatClass(SequenceFileOutputFormat.class);
		FileInputFormat.addInputPath(job, new Path(input));
		FileOutputFormat.setOutputPath(job, new Path(output));
		job.waitForCompletion(true);
	}
	
	public static void testNLineOutput(String input, String output) throws Exception {


		Configuration conf = new Configuration();
		conf.set("mapred.job.tracker", "hdfs://localhost:50001");
		conf.setInt("mapreduce.input.lineinputformat.linespermap",5);
		
		Job job = new Job(conf, "Drug Amount Spent");
		job.setJarByClass(InputFormatSamples.class); // class conmtains mapper and
//		job.setMapOutputKeyClass(Text.class); // map output key class
//		job.setMapOutputValueClass(IntWritable.class);// map output value class
//		job.setOutputKeyClass(Text.class); // output key type in reducer
//		job.setOutputValueClass(IntWritable.class);// output value type in
	
		job.setMapperClass(MapDemohadoop.class);
		job.setReducerClass(Reducesenthil.class);
		job.setNumReduceTasks(1);
		job.setInputFormatClass(NLineInputFormat.class);					
		job.setOutputFormatClass(TextOutputFormat.class);
		FileInputFormat.addInputPath(job, new Path(input));
		FileOutputFormat.setOutputPath(job, new Path(output));
		job.waitForCompletion(true);

	}
	
	
}