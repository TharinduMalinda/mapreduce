package com.iit.bdp.mapreduce;

import com.iit.bdp.mapreduce.util.Util;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

import java.io.IOException;
import java.util.*;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Reducer;

public class CityGroup {

    public static class NeighbourhoodGroupMapper extends
            Mapper<Object, Text, Text, IntWritable> {

        public static final String CITY_COUNTER_GROUP = "cityGroup";
        private final static IntWritable one = new IntWritable(1);
        private Text word = new Text();

        @Override
        public void map(Object key, Text value, Context context)
                throws IOException, InterruptedException {

            Map<String, String> parsed = Util.transRegeion(value.toString());

            String region = parsed.get("city");
            System.out.println("city :" + region);
            word.set(region);
            context.write(word, one);

//            if (region != null && !region.isEmpty()) {
//                    context.getCounter(CITY_COUNTER_GROUP, region).increment(1);
//            }
        }
    }

    public static class WordCountReducer extends Reducer<Text, IntWritable, Text, IntWritable> {

        private IntWritable result = new IntWritable();

        public void reduce(Text key, Iterable<IntWritable> values, Context context)
                throws IOException, InterruptedException {
            int sum = 0;
            for (IntWritable val : values) {
                sum += val.get();
            }
            result.set(sum);
            context.write(key, result);
        }

    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        String[] otherArgs = new GenericOptionsParser(conf, args)
                .getRemainingArgs();
        if (otherArgs.length != 2) {
            System.err.println("Usage: WordCount <in> <out>");
            System.exit(2);
        }
        Job job = Job.getInstance(conf, "word count");
        job.setJarByClass(CityGroup.class);
        job.setMapperClass(NeighbourhoodGroupMapper.class);
        job.setCombinerClass(WordCountReducer.class);
        job.setReducerClass(WordCountReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
        FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
        FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }

//    public static void main(String[] args) throws Exception {
//        Configuration conf = new Configuration();
//        String[] otherArgs = new GenericOptionsParser(conf, args)
//                .getRemainingArgs();
//
//        if (otherArgs.length != 2) {
//            System.err.println("input: cityGroup <in> <out>");
//            System.exit(2);
//        }
//
//        Path input = new Path(otherArgs[0]);
//        Path outputDir = new Path(otherArgs[1]);
//
//        Job job = Job.getInstance(conf, "Count by city Group");
//        job.setJarByClass(CityGroup.class);
//
//        job.setMapperClass(CityGroup.NeighbourhoodGroupMapper.class);
//        job.setNumReduceTasks(0);
//        
//        job.setCombinerClass(WordCountReducer.class);
//        job.setReducerClass(WordCountReducer.class);
//
////        job.setOutputKeyClass(Text.class);
////        job.setOutputValueClass(IntWritable.class);
//        job.setOutputKeyClass(NullWritable.class);
//        job.setOutputValueClass(NullWritable.class);
//        
//        FileInputFormat.addInputPath(job, input);
//        FileOutputFormat.setOutputPath(job, outputDir);
//
//        int code = job.waitForCompletion(true) ? 0 : 1;
//
//        if (code == 0) {
//            for (Counter counter : job.getCounters().getGroup(
//                    CityGroup.NeighbourhoodGroupMapper.CITY_COUNTER_GROUP)) {
//                System.out.println(counter.getDisplayName() + "\t"
//                        + counter.getValue());
//            }
//        }
//
//        // Clean up empty output directory
//        //FileSystem.get(conf).delete(outputDir, true);
//
//        System.exit(code);
//    }
}
