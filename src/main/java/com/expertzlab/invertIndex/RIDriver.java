package com.expertzlab.invertIndex;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

/**
 * Created by gireeshbabu on 24/04/17.
 */
public class RIDriver{


    public static void main(String[] args) throws Exception {
        RIDriver riDriver = new RIDriver();
        Path inp = new Path(args[0]);
        Path outp = new Path(args[1]);
        riDriver.run(inp, outp);

    }

    public void run(Path inp, Path outp) throws Exception {


        Configuration conf = new Configuration();
        Job job = new Job(conf);
        job.setJarByClass(RIDriver.class);
        job.setMapperClass(RIMap.class);
        job.setReducerClass(RIReduce.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);

        FileInputFormat.setInputPaths(job,inp);

        FileOutputFormat.setOutputPath(job,outp);

        if(job.waitForCompletion(true)){
            System.out.println("Job Completed Successfully");
        }
    }
}
