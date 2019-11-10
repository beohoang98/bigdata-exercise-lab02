package com.beohoang.mapreduce;

import com.beohoang.mapreduce.LastFMConstants;
import java.io.IOException;
import java.util.HashSet;
import java.util.Set;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Counters;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class SharedCount {
    IntWritable trackId = new IntWritable();
    IntWritable userId = new IntWritable();

    public enum COUNTERS {
        INVALID_RECORD_COUNT,
    }

    public static class SharedCountMapper extends Mapper<Object, Text, IntWritable, IntWritable> {
        IntWritable trackId = new IntWritable();
        IntWritable isShared = new IntWritable();

        public void map(Object key, Text value, Mapper<Object, Text, IntWritable, IntWritable>.Context context)
                throws IOException, InterruptedException {
            String[] parts = value.toString().split("[|]");
            trackId.set(Integer.parseInt(parts[LastFMConstants.TRACK_ID]));
            isShared.set(Integer.parseInt(parts[LastFMConstants.IS_SHARED]));
            if (parts.length == 5) {
                context.write(trackId, isShared);
            } else {
                // add counter for invalid records
                context.getCounter(COUNTERS.INVALID_RECORD_COUNT).increment(1L);
            }
        }
    }

    public static class SharedCountReducer extends Reducer<IntWritable, IntWritable, IntWritable, IntWritable> {

        public void reduce(IntWritable trackId, Iterable<IntWritable> isShareds,
                Reducer<IntWritable, IntWritable, IntWritable, IntWritable>.Context context)
                throws IOException, InterruptedException {
            int count = 0;
            for (IntWritable isShared : isShareds) {
                count += isShared.get();
            }
            IntWritable total = new IntWritable(count);
            context.write(trackId, total);
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        if (args.length != 2) {
            System.err.println("Usage: uniquelisteners < in >< out >");
            System.exit(2);
        }
        Job job = new Job(conf, "Unique listeners per track");
        job.setJarByClass(SharedCount.class);
        job.setMapperClass(SharedCountMapper.class);
        job.setReducerClass(SharedCountReducer.class);
        job.setOutputKeyClass(IntWritable.class);
        job.setOutputValueClass(IntWritable.class);
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
        Counters counters = job.getCounters();
        System.out.println("No. of Invalid Records :" + counters.findCounter(COUNTERS.INVALID_RECORD_COUNT).getValue());
    }
}
