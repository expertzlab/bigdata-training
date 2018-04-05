package com.expertzlab.mvlense;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import org.junit.Before;
import org.junit.Test;

import java.io.IOException;

import static org.mockito.Mockito.mock;

public class MapperTest {

    MvMapper mapper = new MvMapper();
    Mapper.Context context = mock(Mapper.Context.class);

    @Before
    public void setup(){
    }

    @Test
    public void mapTest() throws IOException, InterruptedException {
        LongWritable key = new LongWritable(1234);
        Text value = new Text("196\t242\t3\t881250949");
        mapper.map(key, value,context);
    }
}
