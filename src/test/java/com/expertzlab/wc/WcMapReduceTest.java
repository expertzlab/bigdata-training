package com.expertzlab.wc;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mrunit.mapreduce.MapReduceDriver;
import org.junit.Test;

import java.io.IOException;

/**
 * Created by admin on 23/07/18.
 */
public class WcMapReduceTest {

    MapReduceDriver<LongWritable,Text,Text,Text,Text, Text> mapReduceDriver;

    @Test
    public void testMapReduce() throws IOException {
        mapReduceDriver = new MapReduceDriver<LongWritable, Text, Text, Text, Text, Text>();
        mapReduceDriver.setMapper(new WcMap());
        mapReduceDriver.setReducer(new WcReduce());
        mapReduceDriver.withInput(new LongWritable(), new Text(
                "cat cat cat mat mat"));
        mapReduceDriver.withOutput(new Text("cat"), new Text("3"));
        mapReduceDriver.withOutput(new Text("mat"), new Text("2"));
        mapReduceDriver.runTest();
    }
}
