
import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import mapper.MapperDemo;
import partitioner.DrugPartitioner;
import reducer.DefaultReducerDemo;

public class CategoryGroupMR {

	private static boolean enableDSMode = false;

	public static void main(String[] args) throws Exception {
		int numReduceTasks = (args.length >= 1) ? Integer.parseInt(args[0]) : 1;
		System.out.println("numReduceTasks::" + numReduceTasks);

		Configuration conf = new Configuration();

		if (enableDSMode)
			conf.set("mapred.job.tracker", "hdfs://localhost:50001");

		Job job = new Job(conf, "Drug Amount Spent");

		if (enableDSMode)
			job.setJarByClass(CategoryGroupMR.class);

		setMapOutputKey(job);
		setDefaultReducer(job);
		setDefaultMapper(job);
		if (numReduceTasks >= 3)
			setCustomPartitioner(job);
		setReducerCount(numReduceTasks, job);

		// default -- inputkey type -- longwritable: valuetype is text
		defaultInputOutputFormat(job);

		if (enableDSMode) {
			setDistributedOutputParam(job);
		} else {
			setLocalOutputParam(job);
		}

		job.waitForCompletion(true);

	}

	private static void setLocalOutputParam(Job job) {
		String input = "/Users/varshika/Ganesan/Hadoop/Workspace/data/data.txt";
		String output = "/Users/varshika/Ganesan/Hadoop/Workspace/data/out_" + System.currentTimeMillis();

		try {

			FileInputFormat.addInputPath(job, new Path(input));
			FileOutputFormat.setOutputPath(job, new Path(output));

		} catch (IOException e) {
			e.printStackTrace();
		}

	}

	private static void setReducerCount(int numReduceTasks, Job job) {
		job.setNumReduceTasks(numReduceTasks);
	}

	private static void defaultInputOutputFormat(Job job) {
		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);
	}

	private static void setMapOutputKey(Job job) {
		// output key type in Mapper
		job.setMapOutputKeyClass(Text.class);
		// output value type in Mapper
		job.setMapOutputValueClass(IntWritable.class);

		// output key type in reducer
		job.setOutputKeyClass(Text.class);
		// output value type in reducer
		job.setOutputValueClass(IntWritable.class);
	}

	private static void setDistributedOutputParam(Job job) throws IOException {
		String input = "/data/data.txt";
		String output = "/data/out_" + System.currentTimeMillis();
		FileInputFormat.addInputPath(job, new Path(input));
		FileOutputFormat.setOutputPath(job, new Path(output));
	}

	private static void setCustomPartitioner(Job job) {
		job.setPartitionerClass(DrugPartitioner.class);
	}

	private static void setDefaultReducer(Job job) {
		job.setReducerClass(DefaultReducerDemo.class);
	}

	private static void setDefaultMapper(Job job) {
		job.setMapperClass(MapperDemo.class);

	}

}
