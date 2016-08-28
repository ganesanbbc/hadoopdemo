
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

public class CategoryGroupMR {

	public static class MapperDemo extends Mapper<LongWritable, Text, Text, IntWritable> {

		@Override
		protected void cleanup(Mapper<LongWritable, Text, Text, IntWritable>.Context context)
				throws IOException, InterruptedException {
			super.cleanup(context);
		}

		@Override
		public void run(Mapper<LongWritable, Text, Text, IntWritable>.Context context)
				throws IOException, InterruptedException {
			super.run(context);
		}

		@Override
		protected void setup(Mapper<LongWritable, Text, Text, IntWritable>.Context context)
				throws IOException, InterruptedException {
			super.setup(context);
		}

		// setup , map, run, cleanup
		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

			String line = value.toString();
			String[] elements = line.split(",");

			Text tx = new Text(elements[2]);
			int i = Integer.parseInt(elements[4]);
			IntWritable it = new IntWritable(i);
			context.write(tx, it);
		}
	}

	public static class ReducerDemo extends Reducer<Text, IntWritable, Text, IntWritable> {

		// setup, reduce, run, cleanup
		// innput - para [150,100]
		
		
		
		public void reduce(Text key, Iterable<IntWritable> values, Context context)
				throws IOException, InterruptedException {
			int sum = 0;
			for (IntWritable val : values) {
				sum += val.get();
			}
			context.write(key, new IntWritable(sum));
		}

		@Override
		protected void cleanup(Reducer<Text, IntWritable, Text, IntWritable>.Context context)
				throws IOException, InterruptedException {
			super.cleanup(context);
		}

		@Override
		public void run(Reducer<Text, IntWritable, Text, IntWritable>.Context context)
				throws IOException, InterruptedException {
			super.run(context);
		}

		@Override
		protected void setup(Reducer<Text, IntWritable, Text, IntWritable>.Context context)
				throws IOException, InterruptedException {
			super.setup(context);
		}
	}

	public static void main(String[] args) throws Exception {

		if (args.length != 3) {
			System.err.println("Insufficient args");
			System.exit(-1);
		}
		
		String input = args[0];
		String output = args[1];
		int numReduceTasks = Integer.parseInt(args[2]);
		
		System.out.println("numReduceTasks::"+numReduceTasks);
		
		Configuration conf = new Configuration();

		conf.set("mapred.job.tracker", "hdfs://localhost:50001");
		// conf.set("DrugName", args[3]);
		Job job = new Job(conf, "Drug Amount Spent");

		job.setJarByClass(CategoryGroupMR.class); // class conmtains mapper and
												// reducer class

		job.setMapOutputKeyClass(Text.class); // map output key class
		job.setMapOutputValueClass(IntWritable.class);// map output value class
		job.setOutputKeyClass(Text.class); // output key type in reducer
		job.setOutputValueClass(IntWritable.class);// output value type in
													// reducer

		job.setMapperClass(MapperDemo.class);
		job.setReducerClass(ReducerDemo.class);
		job.setNumReduceTasks(numReduceTasks);
		job.setInputFormatClass(TextInputFormat.class); // default -- inputkey
														// type -- longwritable
														// : valuetype is text
		job.setOutputFormatClass(TextOutputFormat.class);

		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));

		job.waitForCompletion(true);

	}

}
