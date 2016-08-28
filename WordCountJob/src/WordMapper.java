import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;

public class WordMapper extends MapReduceBase implements Mapper<LongWritable, Text, Text, IntWritable> {

	Map<String,Integer> wordCountMap = new HashMap();
	
	@Override
	public void map(LongWritable key, Text value,
			OutputCollector<Text, IntWritable> output,
			Reporter reporter)
			throws IOException {

		String s = value.toString();
		System.out.println(":::::::::::::::::::::KEY:::"+key.toString());
		System.out.println(":::::::::::::::::::::INPUT FILE:::AS VALUE:::"+s);
		
		
		
		for (String word : s.split(" ")) {
			if (word.length() > 0) {
//				wordCountMap.get(word);
				output.collect(new Text(word), new IntWritable(1));
			}
		}

	}

}
