package mapper;
import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Mapper.Context;

public class MapperDemo extends Mapper<LongWritable, Text, Text, IntWritable> {

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

	public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
		System.out.println("KEY::"+key+"\nVALUES::"+value);
		String line = value.toString();
		String[] elements = line.split(",");

		Text tx = new Text(elements[2]);
		int i = Integer.parseInt(elements[4]);
		IntWritable it = new IntWritable(i);
		context.write(tx, it);
	}
}