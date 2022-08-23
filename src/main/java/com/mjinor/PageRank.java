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

public class PageRank {

    public static class Map extends Mapper<LongWritable, Text, Text, Text> {

        private final Text first_node = new Text();
        private final Text rank = new Text();

        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String line = value.toString();
            StringTokenizer tokenizer = new StringTokenizer(line);
            first_node.set(tokenizer.nextToken());
            rank.set(tokenizer.nextToken());
            String string_of_other_nodes = tokenizer.nextToken();
            String[] other_nodes = string_of_other_nodes.split(",");
            if (other_nodes.length == 0)
                return;
            float temp_rank;
            for (String other_node : other_nodes) {
                temp_rank = Float.parseFloat(rank.toString()) / other_nodes.length;
                context.write(new Text(other_node),new Text(temp_rank + ""));
            }
            context.write(first_node,new Text("[" + string_of_other_nodes));
        }
    }

    public static class Reduce extends Reducer<Text, Text, Text, Text> {

        private float total_rank = 0F;
        private String string_of_other_nodes;

        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            for (Text temp : values) {
                if (temp.toString().startsWith("[")) {
                    string_of_other_nodes = temp.toString().replace("[", "");
                } else {
                    total_rank += Float.parseFloat(temp.toString());
                }
            }
            total_rank = (1F - 0.85F) + (0.85F * total_rank);
            context.write(key,new Text(total_rank + "\t" + string_of_other_nodes));
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