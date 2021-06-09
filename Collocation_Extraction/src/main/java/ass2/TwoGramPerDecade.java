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
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class TwoGramPerDecade {

public static class MapperClass extends Mapper<LongWritable, Text, StringIntWritable, IntWritable> {
    private int decade;
    private String twogram;
    private IntWritable count = new IntWritable(0);
    private StringIntWritable si = new StringIntWritable("",0);

    @Override
    public void map(LongWritable key, Text value, Context context) throws IOException,  InterruptedException {
      StringTokenizer itr = new StringTokenizer(value.toString());
        String utf8EncodedString = null;
        String w1 = null;
        String w2 = null;
      if(itr.countTokens() > 3){
          w1 = itr.nextToken();
          w2 = itr.nextToken();
          twogram = (w1 + " " + w2);
          // convert to utf-8
          byte[] bytes = twogram.getBytes(StandardCharsets.UTF_8);
          utf8EncodedString = new String(bytes, StandardCharsets.UTF_8);
          
          String strYear = itr.nextToken();
          decade = (Integer.parseInt(strYear.substring(0,3) + "0"));
          count.set(Integer.parseInt(itr.nextToken()));
      }
        if (w1 == null || w2 == null){ // just to check
            return;
        }
      si.setDecade(decade);
      si.setTwogram(utf8EncodedString);
      context.write(si, count);
      context.write(new StringIntWritable(w1 + " *", decade), count);
      context.write(new StringIntWritable("* " + w2, decade), count);
    }
  }


  public static class ReducerClass extends Reducer <StringIntWritable, IntWritable, Text, SumDecadeWritable> {
    private Text two_gram = new Text();
    private SumDecadeWritable sumDecadeC1 = new SumDecadeWritable(0,0, 0);
    private int currentN = 0;
    int currentDecade = 0;
    @Override
    public void reduce(StringIntWritable key, Iterable<IntWritable> values, Context context) throws IOException,  InterruptedException {
      int sum = 0;
      int decade = key.getDecade();
      String twogram = key.getTwogram();
//        < 1960, (shlomo, dani) > {3, 5, 8, 1}
//        < (shlomo, *), 1960 > {3, 5, 8, 1}
//        < (*, dani), 1960 > {3, 5, 8, 1}
        // convert to utf-8
        byte[] bytes = twogram.getBytes(StandardCharsets.UTF_8);
        String utf8EncodedString = new String(bytes, StandardCharsets.UTF_8);

      for (IntWritable count : values) {
        sum += count.get();
      }
      if (!twogram.contains("*"))
        currentN += sum;
      if (currentDecade != decade){
            context.write(new Text("*N*"), new SumDecadeWritable(currentN,currentDecade,0) );
            currentN = 0;
            currentDecade = decade;
      }
      two_gram.set(utf8EncodedString);
      sumDecadeC1.setSum(sum);
      sumDecadeC1.setDecade(decade);
      context.write(two_gram, sumDecadeC1 );
//      context.write("N", N );
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
    job.setOutputValueClass(SumDecadeWritable.class);
//    job.setNumReduceTasks(20);
//    job.setInputFormatClass(SequenceFileInputFormat.class);
    FileInputFormat.addInputPath(job, new Path(args[0]));
    FileOutputFormat.setOutputPath(job, new Path(args[1]));
    System.exit(job.waitForCompletion(true) ? 0 : 1);
  }

}

