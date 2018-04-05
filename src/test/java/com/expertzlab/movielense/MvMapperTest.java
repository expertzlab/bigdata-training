package com.expertzlab.movielense;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;

import static org.mockito.Mockito.mock;

/**
 * Created by admin on 24/03/18.
 */
public class MvMapperTest {

    MvMapper mvMapper = new MvMapper();
    Mapper.Context context;

    @Before
    public void setup(){
        context = mock(Mapper.Context.class);
    }

    @Test
    public void mapTest() throws IOException, InterruptedException {
        LongWritable lw = new LongWritable(234);
        Text text = new Text("196\t242\t3\t881250949");
        mvMapper.map(lw,text,context);
        System.out.println(context);
    }
}
