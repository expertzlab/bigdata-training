package com.expertzlab.wc;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mrunit.mapreduce.ReduceDriver;
import org.junit.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * Created by admin on 23/07/18.
 */
public class WcReducerTest {
    ReduceDriver<Text, Text, Text, Text> reduceDriver;

    @Test
    public void testReducer() throws IOException {
        reduceDriver = new ReduceDriver();
        reduceDriver.setReducer(new WcReduce());
        List<Text> values = new ArrayList();
        values.add(new Text("1"));
        values.add(new Text("1"));
        reduceDriver.withInput(new Text("cat"), values);
        reduceDriver.withOutput(new Text("cat"), new Text("2"));
        reduceDriver.runTest();
    }
}
