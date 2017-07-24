package com.expertzlab.wc;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.util.StringUtils;

import java.io.IOException;

public class WcMap extends Mapper<LongWritable, Text, Text,Text> {

    private Text documentId;
    private Text word = new Text();

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        for(String token: StringUtils.split(value.toString(),' ')){
            word.set(token);
            context.write(word, new Text("1"));
            System.out.println("word:"+word.toString()+", docid:"+documentId);
        }
    }
}