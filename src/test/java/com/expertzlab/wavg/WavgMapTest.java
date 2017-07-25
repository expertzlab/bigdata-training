package com.expertzlab.wavg;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.junit.Test;

import java.io.IOException;

import static org.mockito.Mockito.*;

/**
 * Created by gireeshbabu on 25/07/17.
 */
public class WavgMapTest {


    WavgMap mapper = new WavgMap();

    @Test
    public void mapTest() throws IOException, InterruptedException {
        LongWritable lw = new LongWritable(2345);
        Text text = new Text("cat\t2");
        Mapper.Context context = mock(Mapper.Context.class);
        mapper.map(lw,text,context);
    }
}
