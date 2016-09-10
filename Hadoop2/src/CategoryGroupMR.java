
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

public class CategoryGroupMR {

	public static void main(String[] args) throws Exception {

		if (args.length != 1) {
			System.err.println("Insufficient args");
			System.exit(-1);
		}

		String input = "/data/data.txt";
		
		String output = "/data/out_" + System.currentTimeMillis();
		int numReduceTasks = Integer.parseInt(args[0]);

		System.out.println("numReduceTasks::" + numReduceTasks);

		Configuration conf = new Configuration();
		conf.set("mapred.job.tracker", "hdfs://localhost:50001");

		Job job = new Job(conf, "Drug Amount Spent");
		job.setJarByClass(CategoryGroupMR.class);

		// output key type in Mapper
		job.setMapOutputKeyClass(Text.class);
		// output value type in Mapper
		job.setMapOutputValueClass(IntWritable.class);

		// output key type in reducer
		job.setOutputKeyClass(Text.class);
		// output value type in reducer
		job.setOutputValueClass(IntWritable.class);

		job.setNumReduceTasks(numReduceTasks);
		
		
		job.setMapperClass(MapperDemo.class);
		job.setReducerClass(ReducerDemo.class);
		
		//Default HashPartitioner<K2, V2>
		job.setPartitionerClass(DrugPartitioner.class);
		
		
		
		// default -- inputkey type -- longwritable: valuetype is text
		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);

		FileInputFormat.addInputPath(job, new Path(input));
		FileOutputFormat.setOutputPath(job, new Path(output));

		job.waitForCompletion(true);

	}

}
