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

public class ListenCount {
    public enum COUNTERS {
        INVALID_RECORD_COUNT,
    }

    public static class ListenCountMapper extends Mapper<Object, Text, IntWritable, IntWritable> {
        IntWritable trackId = new IntWritable();
        IntWritable one = new IntWritable(1);
        IntWritable zero = new IntWritable(0);

        public void map(Object key, Text value, Mapper<Object, Text, IntWritable, IntWritable>.Context context)
                throws IOException, InterruptedException {
            String[] parts = value.toString().split("[|]");

            Configuration conf = context.getConfiguration();

            String type = conf.get("type", "all").trim();
            String skipped = conf.get("skipped", "all").trim();

            trackId.set(Integer.parseInt(parts[LastFMConstants.TRACK_ID]));
            int dataRadio = Integer.parseInt(parts[LastFMConstants.RADIO]);
            int dataSkipped = Integer.parseInt(parts[LastFMConstants.IS_SKIPPED]);

            boolean isCount = ( (dataRadio == 0 ^ type.equals("radio") ) || type.equals("all") ) // XOR
                    && ( (dataSkipped == 0 ^ skipped.equals("skipped") ) || skipped.equals("all") );

            if (parts.length == 5) {
                if (isCount) {
                    context.write(trackId, one);
                } else {
                    context.write(trackId, zero);
                }
            } else {
                // add counter for invalid records
                context.getCounter(COUNTERS.INVALID_RECORD_COUNT).increment(1L);
            }
        }
    }

    public static class ListenCountReducer extends Reducer<IntWritable, IntWritable, IntWritable, IntWritable> {

        public void reduce(IntWritable trackId, Iterable<IntWritable> counts,
                Reducer<IntWritable, IntWritable, IntWritable, IntWritable>.Context context)
                throws IOException, InterruptedException {
            int sum = 0;
            for (IntWritable c : counts) {
                sum += c.get();
            }

            IntWritable total = new IntWritable(sum);
            context.write(trackId, total);
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        if (args.length < 2) {
            System.err.println("Usage: ListenCount <in> <out> <radio|no|all> <skipped|no|all>");
            System.exit(2);
        }
        String type = args.length >= 3 ? new String(args[2]) : "all";
        String skipped = args.length >= 4 ? new String(args[3]) : "all";
        conf.set("type", type);
        conf.set("skipped", skipped);
        System.out.println("Type: " + type);
        System.out.println("skip type: " + skipped);

        Job job = new Job(conf, "ListenCount");
        job.setJarByClass(ListenCount.class);
        job.setMapperClass(ListenCountMapper.class);
        job.setReducerClass(ListenCountReducer.class);
        job.setOutputKeyClass(IntWritable.class);
        job.setOutputValueClass(IntWritable.class);
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
        Counters counters = job.getCounters();
        System.out.println("No. of Invalid Records :" + counters.findCounter(COUNTERS.INVALID_RECORD_COUNT).getValue());
    }
}
