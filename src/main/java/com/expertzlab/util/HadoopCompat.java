package com.expertzlab.util;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.JobContext;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;

/**
 * Created by gireeshbabu on 11/05/17.
 */
public class HadoopCompat {

    private static Method GET_CONFIGURATION_METHOD;

    static {
        final String PACKAGE = "org.apache.hadoop.mapreduce";
        try {
            GET_CONFIGURATION_METHOD = Class.forName(PACKAGE + ".JobContext")
                    .getMethod("getConfiguration");
        } catch (NoSuchMethodException e) {
            System.out.println("Nosuch method Exception"+e);
        } catch (ClassNotFoundException e) {
            System.out.println("ClassNotFound Exception"+e);
        }
    }

    /**
     * Invoke getConfiguration() on JobContext. Works with both
     * Hadoop 1 and 2.
     */
    public static Configuration getConfiguration(JobContext context) {
        return (Configuration) invoke(GET_CONFIGURATION_METHOD, context);
    }

    /**
     * Invokes a method and rethrows any exception as runtime excetpions.
     */
    private static Object invoke(Method method, Object obj, Object... args) {
        try {
            return method.invoke(obj, args);
        } catch (IllegalAccessException e) {
            throw new IllegalArgumentException("Can't invoke method " + method.getName(), e);
        } catch (InvocationTargetException e) {
            throw new IllegalArgumentException("Can't invoke method " + method.getName(), e);
        }
    }
}
