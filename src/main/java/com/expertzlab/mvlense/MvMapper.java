package com.expertzlab.mvlense;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class MvMapper  extends Mapper<LongWritable, Text, Text, Text> {

    Text keyOut = new Text();
    Text valueOut = new Text();

    protected void map(LongWritable key, Text value,
                       Context context) throws IOException, InterruptedException {

        String[] strArray = value.toString().split("\t");
        String mvId = strArray[1];
        String rating = strArray[2];
        keyOut.set(mvId);
        valueOut.set(rating);
        context.write(keyOut,valueOut);
    }

}
