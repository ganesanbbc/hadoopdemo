package org.samples.mapreduce.training;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer.Context;

public class MapDemohadoop extends
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