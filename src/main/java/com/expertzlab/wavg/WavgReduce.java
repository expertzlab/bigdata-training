package com.expertzlab.wavg;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

/**
 * Created by gireeshbabu on 24/04/17.
 */
public class WavgReduce extends Reducer<Text, Text, Text, Text> {

    private Map<String,String> map = new HashMap<String,String>();

    public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        System.out.println("key:"+key+", values:"+values);

        int totalcount = 0;
        int average = 0;
        for(Text val: values){
            String[] array = StringUtils.split(val.toString(),":");
            map.put(array[0],array[1]);
            totalcount += Integer.parseInt(array[1]);
        }

        for(Map.Entry e: map.entrySet()){

            average = (Integer.parseInt((String)e.getValue())/totalcount)*100;
            context.write(new Text((String)e.getKey()),new Text(""+average));
        }


    }
}
