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

public class TrustThreshold {

    public static class Map extends Mapper<LongWritable, Text, Text, Text> {

        private final Text first_node = new Text();
        private final Text rank = new Text();
        private final Text other_nodes = new Text();
        private final static int THRESHOLD = 9;

        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String line = value.toString();
            StringTokenizer tokenizer = new StringTokenizer(line);
            first_node.set(tokenizer.nextToken());
            rank.set(tokenizer.nextToken());
            other_nodes.set(tokenizer.nextToken());
            if (Float.parseFloat(rank.toString()) > THRESHOLD) {
                context.write(first_node,new Text(rank.toString() + "\t" + other_nodes.toString()));
            }
        }
    }

    public static class Reduce extends Reducer<Text, Text, Text, Text> {

        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            Text v = new Text();
            v.set(values.iterator().next());
//            for (Text value : values) {
//                v.set(value);
//                break;
//            }
            context.write(key,v);
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();

        Job job = new Job(conf, "threshold");
        job.setJarByClass(TrustThreshold.class);

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