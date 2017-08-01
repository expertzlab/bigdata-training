package com.expertzlab.wavg;

import com.expertzlab.wc.WcMap;
import com.expertzlab.wc.WcReduce;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

/**
 * Created by gireeshbabu on 24/04/17.
 */
public class WavgDriver {


    public static void main(String[] args) throws Exception {
        WavgDriver riDriver = new WavgDriver();
        Path inp = new Path(args[0]);
        Path inter = new Path("inter");
        Path outp = new Path(args[1]);
        riDriver.run(inp, inter, outp);

    }

    public void run(Path inp, Path inter, Path outp) throws Exception {


        Configuration conf = new Configuration();


        Job job1 = new Job(conf);
        job1.setJarByClass(WavgDriver.class);
        job1.setMapperClass(WcMap.class);
        job1.setReducerClass(WcReduce.class);
        job1.setMapOutputKeyClass(Text.class);
        job1.setMapOutputValueClass(Text.class);
        FileInputFormat.setInputPaths(job1,inp);
        FileOutputFormat.setOutputPath(job1,inter);

        if(job1.waitForCompletion(true)){
            System.out.println("Job 1 Completed Successfully");
        }

        Job job2 = new Job(conf);
        job2.setJarByClass(WavgDriver.class);
        job2.setMapperClass(WavgMap.class);
        job2.setReducerClass(WavgReduce.class);
        job2.setMapOutputKeyClass(Text.class);
        job2.setMapOutputValueClass(Text.class);
        FileInputFormat.setInputPaths(job2,inter);
        FileOutputFormat.setOutputPath(job2,outp);

        if(job2.waitForCompletion(true)){
            System.out.println("Job 2 Completed Successfully");
        }

    }
}
