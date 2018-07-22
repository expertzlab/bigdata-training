package com.expertzlab.invertIndex;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mrunit.MapReduceDriver;
import org.apache.hadoop.mrunit.mapreduce.MapDriver;
import org.apache.hadoop.mrunit.mapreduce.ReduceDriver;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * Created by admin on 17/07/18.
 */
public class RIMapTest {

    MapDriver<LongWritable, Text, Text, Text> mapDriver;
    ReduceDriver<Text, Text, Text, Text> reduceDriver;
    MapReduceDriver<LongWritable,Text,Text,Text,Text, Text> mapReduceDriver;

    @Before
    public void setUp() {
        Mapper<LongWritable, Text, Text,Text> mapper = new RIMap();
        mapDriver = new MapDriver();
        mapDriver.setMapper(mapper);

        Reducer reducer = new RIReduce();
        reduceDriver = new ReduceDriver<>();
        reduceDriver.setReducer(reducer);
    }

    /*
    @Test
    public void testMapperWithSingleKeyAndValue() throws Exception {
        final LongWritable inputKey = new LongWritable(0);
        final Text inputValue = new Text("cat");

        final Text outputKey = new Text("cat");
        final Text outputValue = new Text("doc1");

        mapDriver.withInput(inputKey, inputValue);
        mapDriver.withOutput(outputKey, outputValue);
        mapDriver.runTest();

    }
    */

    @Test
    public void testReducer() throws IOException {
        List<Text> values = new ArrayList<>();
        values.add(new Text("1"));
        values.add(new Text("1"));
        reduceDriver.withInput(new Text("cat"), values);
        reduceDriver.withOutput(new Text("cat"), new Text("2"));
        reduceDriver.runTest();
    }

    /*
    @Test
    public void testMapReduce() throws IOException {
        mapReduceDriver.withInput(new LongWritable(), new Text(
                "655209;1;796764372490213;804422938115889;6"));
        List<Text> values = new ArrayList<Text>();
        values.add(new Text("1"));
        values.add(new Text("1"));
        mapReduceDriver.withOutput(new Text("6"), new Text("2"));
        mapReduceDriver.runTest();
    }

    */
}
