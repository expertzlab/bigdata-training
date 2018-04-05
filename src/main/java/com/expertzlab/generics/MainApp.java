package com.expertzlab.generics;

import org.apache.hadoop.io.Text;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Created by admin on 21/03/18.
 */
public class MainApp {

    public static void main(String[] args) throws InstantiationException, IllegalAccessException {

        List a = new ArrayList();
        a.add(1);
        a.add(1);
        Iterable<Text> v = a;
        for(Object i: a){
            System.out.println(i);
        }
    }
}
