package com.expertzlab.mvlense;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.DFSClient;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

public class MvDriver {

    public static void main(String[] args) throws InterruptedException, IOException, ClassNotFoundException {
        String in = args[0];
        String out = args[1];

        Path pin = new Path(in);
        Path pout = new Path(out);
        run(pin, pout);

    }

    public static void run(Path in, Path out) throws IOException, ClassNotFoundException, InterruptedException {

        Configuration conf = new Configuration();
        Job job = new Job(conf);
        job.setJarByClass(MvDriver.class);
        job.setMapperClass(MvMapper.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);

        FileInputFormat.setInputPaths(job,in);
        FileOutputFormat.setOutputPath(job,out);

        if(job.waitForCompletion(true)){
            System.out.println("Completed....");
        }
    }

}
