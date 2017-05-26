package com.expertzlab.json;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

public final class JsonMap extends Configured implements Tool {
  private static final Logger log = LoggerFactory.getLogger
      (JsonMap.class);

  public static class Map extends Mapper<LongWritable, MapWritable,
      Text, Text> {

    @Override
    protected void map(LongWritable key, MapWritable value,
                       Context context)
        throws
        IOException, InterruptedException {

      for (java.util.Map.Entry<Writable, Writable> entry : value
          .entrySet()) {
        context.write((Text) entry.getKey(), (Text) entry.getValue());
      }
    }
  }

  /**
   * Main entry point for the example.
   *
   * @param args arguments
   * @throws Exception when something goes wrong
   */
  public static void main(final String[] args) throws Exception {
    int res = ToolRunner.run(new Configuration(), new JsonMap(), args);
    System.exit(res);
  }

  /**
   * The MapReduce driver - setup and launch the job.
   *
   * @param args the command-line arguments
   * @return the process exit code
   * @throws Exception if something goes wrong
   */
  public int run(final String[] args) throws Exception {

    Path inputPath = new Path(args[0]);
    Path outputPath = new Path(args[1]);

    Configuration conf = super.getConf();
    Job job = new Job(conf);
    job.setJarByClass(JsonMap.class);
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(Text.class);
    job.setMapperClass(Map.class);
    job.setInputFormatClass(JsonInputFormat.class);
    job.setNumReduceTasks(0);
    job.setOutputFormatClass(TextOutputFormat.class);

    FileInputFormat.setInputPaths(job, inputPath);
    FileOutputFormat.setOutputPath(job, outputPath);

    if (job.waitForCompletion(true)) {
      return 0;
    }
    return 1;
  }
}
