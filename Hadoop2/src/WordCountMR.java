

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

public class WordCountMR {

	public static class MapperDemo extends
			Mapper<LongWritable, Text, Text, IntWritable> {

		// setup , map, run, cleanup
		public void map(LongWritable key, Text value, Context context)
				throws IOException, InterruptedException {
		
			String line = value.toString();
			String[] elements = line.split(",");

				Text tx = new Text(elements[2]);
				int i = Integer.parseInt(elements[4]);
				IntWritable it = new IntWritable(i);
				context.write(tx, it);
		}
	}

	public static class ReducerDemo extends
			Reducer<Text, IntWritable, Text, IntWritable> {

		// setup, reduce, run, cleanup
		// innput - para [150,100]
		public void reduce(Text key, Iterable<IntWritable> values,
				Context context) throws IOException, InterruptedException {
			int sum = 0;
			for (IntWritable val : values) {
				sum += val.get();
			}
			context.write(key, new IntWritable(sum));
		}
	}

	public static void main(String[] args) throws Exception {

		if (args.length != 2) {
			System.err.println("Insufficient args");
			System.exit(-1);
		}
		Configuration conf = new Configuration();

		conf.set("mapred.job.tracker", "hdfs://localhost:50001");
//		conf.set("DrugName", args[3]);
		Job job = new Job(conf, "Drug Amount Spent");

		job.setJarByClass(WordCountMR.class); // class conmtains mapper and
												// reducer class
		
		job.setMapOutputKeyClass(Text.class); // map output key class
		job.setMapOutputValueClass(IntWritable.class);// map output value class
		job.setOutputKeyClass(Text.class); // output key type in reducer
		job.setOutputValueClass(IntWritable.class);// output value type in
													// reducer

		job.setMapperClass(MapperDemo.class);
		job.setReducerClass(ReducerDemo.class);
		job.setNumReduceTasks(1);
		job.setInputFormatClass(TextInputFormat.class); // default -- inputkey
														// type -- longwritable
														// : valuetype is text
		job.setOutputFormatClass(TextOutputFormat.class);

		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));

		job.waitForCompletion(true);

	}

}