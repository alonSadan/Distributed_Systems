package ass2;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.StringTokenizer;

//import org.apache.giraph.writable.tuple.PairWritable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class WordCount {

//  java.io.IOException: Type mismatch in key from map: expected Text, received PairWritable
//  java.io.IOException: Initialization of all the collectors failed. Error in last collector was :class org.apache.giraph.writable.tuple.PairWritable
//  java.io.IOException: Initialization of all the collectors failed. Error in last collector was :class ass2.StringIntWritable
    public static class MapperClass extends Mapper<LongWritable, Text, StringIntWritable, IntWritable> { // writable array for twogram and decade
    private int decade;
    private String twogram;
    private IntWritable count = new IntWritable(0);
    private StringIntWritable si = new StringIntWritable("",0);

    @Override
    public void map(LongWritable key, Text value, Context context) throws IOException,  InterruptedException {
      StringTokenizer itr = new StringTokenizer(value.toString());
        String utf8EncodedString = null;
      if(itr.countTokens() > 3){
          twogram = (itr.nextToken() + " " + itr.nextToken());
          // convert to utf-8
          byte[] bytes = twogram.getBytes(StandardCharsets.UTF_8);
          utf8EncodedString = new String(bytes, StandardCharsets.UTF_8);
          
          String strYear = itr.nextToken();
          decade = (Integer.parseInt(strYear.substring(0,3) + "0"));
          count.set(Integer.parseInt(itr.nextToken()));
      }
      si.setDecade(decade);
      si.setTwogram(utf8EncodedString);
      context.write(si, count);
    }
  }


  public static class ReducerClass extends Reducer <StringIntWritable, IntWritable, Text, IntIntWritable> {
    private Text two_gram = new Text();
    private IntIntWritable sumDecade = new IntIntWritable(0,0);
    @Override
    public void reduce(StringIntWritable key, Iterable<IntWritable> values, Context context) throws IOException,  InterruptedException {
      int sum = 0;
      int decade = key.getDecade();
      String twogram = key.getTwogram();
        // convert to utf-8
        byte[] bytes = twogram.getBytes(StandardCharsets.UTF_8);
        String utf8EncodedString = new String(bytes, StandardCharsets.UTF_8);

      for (IntWritable count : values) {
        sum += count.get();
      }
      two_gram.set(utf8EncodedString);
      sumDecade.setSum(sum);
      sumDecade.setDecade(decade);
      context.write(two_gram, sumDecade );
    }
  }

//    public static class PartitionerClass extends Partitioner<Text, IntWritable> {
//      @Override
//      public int getPartition(Text key, IntWritable value, int numPartitions) {
//        return key.hashCode() % numPartitions;
//      }
//    }

//    static void fix() {
//        String OS = System.getProperty("os.name").toLowerCase();
//
//        if (OS.contains("win")) {
//            System.setProperty("hadoop.home.dir", "C:\\Users\\yotam\\Desktop\\win_utils");
//        } else {
//            System.setProperty("hadoop.home.dir", "/");
//        }
//    }

//    Method threw 'java.lang.IllegalStateException' exception. Cannot evaluate org.apache.hadoop.mapreduce.Job.toString()

 public static void main(String[] args) throws Exception {
//    fix();
    Configuration conf = new Configuration();
    Job job = Job.getInstance(conf, "word_count");
    job.setJarByClass(WordCountInternet.class);
    job.setMapperClass(MapperClass.class);

//    job.setPartitionerClass(PartitionerClass.class);
//    job.setCombinerClass(ReducerClass.class);

    job.setReducerClass(ReducerClass.class);
    job.setMapOutputKeyClass(StringIntWritable.class);
    job.setMapOutputValueClass(IntWritable.class);
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(IntIntWritable.class);
//    job.setNumReduceTasks(20);
    FileInputFormat.addInputPath(job, new Path(args[0]));
    FileOutputFormat.setOutputPath(job, new Path(args[1]));
    System.exit(job.waitForCompletion(true) ? 0 : 1);
  }

}

