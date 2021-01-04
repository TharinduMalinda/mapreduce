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
import java.util.Map;

public class MatchResults {

    public static class EverydayAvailabilityMapper extends
            Mapper<Object, Text, NullWritable, NullWritable> {

        public static final String WIN_COUNTER_GROUP = "matchWinCounter";
        public static final String DRAW_COUNTER_GROUP = "matchdrowCounter";

        @Override
        public void map(Object key, Text value, Context context)
                throws IOException, InterruptedException {

            Map<String, String> parsed = Util.match_result(value.toString());
            String match_result = parsed.get("result");
            //System.out.println("opendays :" + match_result);

            if (match_result.equals("WIN")) {
                context.getCounter(WIN_COUNTER_GROUP,match_result+"").increment(1);
            }else{
                context.getCounter(DRAW_COUNTER_GROUP,match_result+"").increment(1);
            }

        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        String[] otherArgs = new GenericOptionsParser(conf, args)
                .getRemainingArgs();

        if (otherArgs.length != 2) {
            System.err.println("input: match results <in> <out>");
            System.exit(2);
        }

        Path input = new Path(otherArgs[0]);
        Path outputDir = new Path(otherArgs[1]);

        Job job = Job.getInstance(conf, "match results");
        job.setJarByClass(MatchResults.class);

        job.setMapperClass(MatchResults.EverydayAvailabilityMapper.class);
        job.setNumReduceTasks(0);

        job.setOutputKeyClass(NullWritable.class);
        job.setOutputValueClass(NullWritable.class);

        FileInputFormat.addInputPath(job, input);
        FileOutputFormat.setOutputPath(job, outputDir);

        int code = job.waitForCompletion(true) ? 0 : 1;

        if (code == 0) {
            for (Counter counter : job.getCounters().getGroup(EverydayAvailabilityMapper.WIN_COUNTER_GROUP)) {
                System.out.println("The total number of matches played, ended with a result :" + "\t"
                        + counter.getValue());
            }
            
            for (Counter counter2 : job.getCounters().getGroup(EverydayAvailabilityMapper.DRAW_COUNTER_GROUP)) {
                System.out.println("The total number of matches played, ended with a drawn :" + "\t"
                        + counter2.getValue());
            }
        }

        // Clean up empty output directory
        FileSystem.get(conf).delete(outputDir, true);

        System.exit(code);
    }
}
