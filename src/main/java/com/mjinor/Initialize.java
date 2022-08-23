package com.mjinor;

import java.io.IOException;
import java.util.*;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

public class Initialize {

	public static class Map extends Mapper<LongWritable, Text, Text, Text> {

		private final Text first_node = new Text();
		private final Text other_node = new Text();

		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			String line = value.toString();
			StringTokenizer tokenizer = new StringTokenizer(line);
			first_node.set(tokenizer.nextToken());
			other_node.set(tokenizer.nextToken());
			context.write(first_node,other_node);
		}
	}

	public static class Reduce extends Reducer<Text, Text, Text, Text> {

		private static final float PAGE_RANK = 10.0F;

		public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
			StringBuilder nodes = new StringBuilder(PAGE_RANK + "\t");
			for (Text temp : values) {
				nodes.append(temp.toString());
				if (values.iterator().hasNext()) {
					nodes.append(",");
				}
			}
			context.write(new Text(key.toString()),new Text(nodes.toString()));
		}
	}

	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();

		Job job = new Job(conf, "amazon");
		job.setJarByClass(Initialize.class);

		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);

		job.setMapperClass(Map.class);
		job.setReducerClass(Reduce.class);

		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);

		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));

		job.waitForCompletion(true);
	}

}