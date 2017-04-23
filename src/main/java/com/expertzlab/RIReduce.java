package com.expertzlab;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.HashSet;

/**
 * Created by gireeshbabu on 24/04/17.
 */
public class RIReduce extends Reducer<Text, Text, Text, Text> {

    private Text docIds = new Text();

    public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        System.out.println("key:"+key+", values:"+values);
        HashSet<String> uniqueDocIds = new HashSet<String>();
        for(Text docId: values){
            uniqueDocIds.add(docId.toString());
        }
        docIds.set(new Text(uniqueDocIds.toString()));
        context.write(key,docIds);
    }
}
