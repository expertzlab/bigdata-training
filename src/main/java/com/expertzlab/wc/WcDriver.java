package com.expertzlab.wc;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

/**
 * Created by gireeshbabu on 24/04/17.
 */
public class WcDriver {


    public static void main(String[] args) throws Exception {
        WcDriver riDriver = new WcDriver();
        Path inp = new Path(args[0]);
        Path outp = new Path(args[1]);
        riDriver.run(inp, outp);

    }

    public void run(Path inp, Path outp) throws Exception {


        Configuration conf = new Configuration();
        Job job = new Job(conf);
        job.setJarByClass(WcDriver.class);
        job.setMapperClass(WcMap.class);
        job.setReducerClass(WcReduce.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);

        FileInputFormat.setInputPaths(job,inp);

        FileOutputFormat.setOutputPath(job,outp);

        if(job.waitForCompletion(true)){
            System.out.println("Job Completed Successfully");
        }
    }
}
