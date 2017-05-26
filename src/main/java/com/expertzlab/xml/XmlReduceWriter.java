package com.expertzlab.xml;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.IOException;

public final class XmlReduceWriter extends Configured implements Tool {

  public static class Reduce
      extends Reducer<Text, Text, Text, Text> {

    @Override
    protected void setup(
        Context context)
        throws IOException, InterruptedException {
      context.write(new Text("<configuration>"), null);
    }

    @Override
    protected void cleanup(
        Context context)
        throws IOException, InterruptedException {
      context.write(new Text("</configuration>"), null);
    }

    private Text outputKey = new Text();
    public void reduce(Text key, Iterable<Text> values,
                       Context context)
        throws IOException, InterruptedException {
      for (Text value : values) {
        outputKey.set(constructPropertyXml(key, value));
        context.write(outputKey, null);
      }
    }

    public static String constructPropertyXml(Text name, Text value) {
      return String.format(
          "<property><name>%s</name><value>%s</value></property>",          name, value);
    }
  }

  /**
   * Main entry point for the example.
   *
   * @param args arguments
   * @throws Exception when something goes wrong
   */
  public static void main(final String[] args) throws Exception {
    int res = ToolRunner.run(new Configuration(), new XmlReduceWriter(), args);
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
    job.setJarByClass(XmlReduceWriter.class);
    job.setReducerClass(Reduce.class);
    job.setInputFormatClass(KeyValueTextInputFormat.class);

    job.setMapOutputKeyClass(Text.class);
    job.setMapOutputValueClass(Text.class);

    FileInputFormat.setInputPaths(job, inputPath);
    FileOutputFormat.setOutputPath(job, outputPath);

    if (job.waitForCompletion(true)) {
      return 0;
    }
    return 1;
  }
}
