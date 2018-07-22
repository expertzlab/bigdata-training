package com.expertzlab.wc;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mrunit.mapreduce.MapDriver;
import org.junit.Test;

import java.io.IOException;

/**
 * Created by admin on 18/07/18.
 */
public class WcMapperTest {

    MapDriver<LongWritable, Text, Text, Text> mapDriver;
    WcMap mapper = new WcMap();

    @Test
    public void testMapperWithSingleWordLine() throws IOException, InterruptedException {

        mapDriver = new MapDriver<LongWritable, Text, Text, Text>();
        mapDriver.setMapper(mapper);

        LongWritable ikey = new LongWritable(234);
        Text ivalue = new Text("cat");

        Text outKey = new Text("cat");
        Text outValue = new Text("1");

        mapDriver.withInput(ikey, ivalue);
        mapDriver.withOutput(outKey,outValue);
        mapDriver.runTest();
    }

}
