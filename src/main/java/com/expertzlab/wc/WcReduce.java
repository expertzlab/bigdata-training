package com.expertzlab.wc;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.HashSet;

/**
 * Created by gireeshbabu on 24/04/17.
 */
public class WcReduce extends Reducer<Text, Text, Text, Text> {

    public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        System.out.println("key:"+key+", values:"+values);

        int sum = 0;
        for(Text docId: values){
            sum++;
        }
        context.write(key,new Text(""+sum));
    }
}
