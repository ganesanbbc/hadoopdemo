
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class WordCount extends Configuration implements Tool {

	@Override
	public Configuration getConf() {
		return null;
	}

	@Override
	public void setConf(Configuration arg0) {

	}

	@Override
	public int run(String[] arg0) throws Exception {

		if (arg0.length < 2) {
			System.out.println("Please give ip & op file");
			return -1;
		}

		String inputFile = arg0[0];
		String outputFile = arg0[1];

		JobConf conf = new JobConf(WordCount.class);
		FileInputFormat.setInputPaths(conf, new Path(inputFile));
		FileOutputFormat.setOutputPath(conf, new Path(outputFile));

		conf.setMapperClass(WordMapper.class);
		conf.setReducerClass(WordReducer.class);

		conf.setMapOutputKeyClass(Text.class);
		conf.setMapOutputValueClass(IntWritable.class);

		conf.setOutputKeyClass(Text.class);
		conf.setOutputValueClass(IntWritable.class);

		JobClient.runJob(conf);

		return 0;
	}

	public static void main(String a[]) throws Exception {
		int exitCode = ToolRunner.run(new WordCount(), a);
		System.exit(exitCode);
	}

}
