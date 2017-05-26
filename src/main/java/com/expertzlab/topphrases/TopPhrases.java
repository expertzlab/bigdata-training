package com.expertzlab.topphrases;

import com.expertzlab.invertIndex.RIDriver;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.StringUtils;

import java.io.IOException;

public class TopPhrases {

    public static void main(String[] args) throws Exception {
        RIDriver riDriver = new RIDriver();
        Path inp = new Path(args[0]);
        Path outp = new Path(args[1]);
        riDriver.run(inp, outp);

    }

    public void run(Path inp, Path outp) throws Exception {

        Configuration conf = new Configuration();
        Job job = new Job(conf);
        job.setJarByClass(TopPhrases.class);
        job.setMapperClass(TPMap.class);
        job.setReducerClass(TPReduce.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);
        FileInputFormat.setInputPaths(job,inp);
        FileOutputFormat.setOutputPath(job,outp);

        if(job.waitForCompletion(true)){
            System.out.println("Job Completed Successfully");
        }
    }

    class TPMap extends Mapper<LongWritable, Text, Text, IntWritable> {

        private Text word = new Text();
        private IntWritable one = new IntWritable(1);


        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

            for(String token: StringUtils.split(value.toString(),'|')){
                word.set(token);
                context.write(word, one);
            }
        }
    }

    public class TPReduce extends Reducer<Text, IntWritable, Text, IntWritable> {

        private IntWritable result = new IntWritable();

        @Override
        public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {

            int sum = 0;
            for (IntWritable val : values) {
                sum += val.get();
            }
            result.set(sum);
            context.write(key, result);
        }
    }
}