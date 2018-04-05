package com.expertzlab.movielense;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;

import java.io.IOException;

/**
 * Created by admin on 24/03/18.
 */
public class MvMapper extends Mapper<LongWritable,Text,Text,Text> {

    Text keyText = new Text();
    Text valueText = new Text();

    protected void map(LongWritable key, Text value, Mapper.Context context) throws IOException, InterruptedException {

        String valStr = value.toString();
        String[] strArray = valStr.split("\t");
        String movieId = strArray[1];
        String rating = strArray[2];
        keyText.set(movieId);
        valueText.set(rating);
        context.write(keyText, valueText);
    }
}
