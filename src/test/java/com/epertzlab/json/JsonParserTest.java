package com.epertzlab.json;

import com.expertzlab.json.JsonInputFormat;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.Text;
import org.junit.Test;

import java.io.IOException;

/**
 * Created by gireeshbabu on 31/07/17.
 */
public class JsonParserTest {


    @Test
    public void testDecodeLineToJson() throws IOException {

        Text lineText = new Text();
        lineText.set("[{\"name\": \"ajai\",\"age\": \"23\", \"address\": \"34\", \"pin\": \"68382\"},");
        MapWritable mapWritable = new MapWritable();
        JsonInputFormat.JsonRecordReader.decodeLineToJson(lineText,mapWritable);
        for(MapWritable.Entry e: mapWritable.entrySet()){
            System.out.println("key ="+e.getKey()+ " \n value = "+ e.getValue());
        }
    }
}
