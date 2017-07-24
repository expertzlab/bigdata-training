package com.expertzlab.wavg;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.util.StringUtils;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

public class WavgMap extends Mapper<LongWritable, Text, Text,Text> {

    private Text documentId;
    private Text rkey = new Text();
    private Text rValue = new Text();



    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String[] array = StringUtils.split(value.toString());
        rkey.set("average");
        rValue.set(array[0]+":"+array[1]);
        context.write(rkey, rValue);
    }
}