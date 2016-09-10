import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class DefaultReducerDemo extends Reducer<Text, IntWritable, Text, IntWritable> {

	public void reduce(Text key, IntWritable values, Context context)
			throws IOException, InterruptedException {
		context.write(key, values);
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