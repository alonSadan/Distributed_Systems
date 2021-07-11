package ass2;

import java.io.IOException;
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

public static class MapperClass extends Mapper<LongWritable, Text, StringStringIntWritable, IntWritable> {
    private int decade;
    private String twogram;
    private IntWritable count = new IntWritable(0);
    private StringStringIntWritable ssi = new StringStringIntWritable("","",0);

    @Override
    public void map(LongWritable key, Text value, Context context) throws IOException,  InterruptedException {
      StringTokenizer itr = new StringTokenizer(value.toString());
        String w1 = null;
        String w2 = null;
      if(itr.countTokens() > 3){
          w1 = itr.nextToken();
          w2 = itr.nextToken();
          twogram = (w1 + " " + w2);
          String strYear = itr.nextToken();
          decade = (Integer.parseInt(strYear.substring(0,3) + "0"));
          count.set(Integer.parseInt(itr.nextToken()));
      }
        if (w1 == null || w2 == null){ // just to check
            return;
        }
      ssi.setTwogram(twogram);
      ssi.setSecondPair(twogram);
      ssi.setDecade(decade);
      context.write(ssi, count);
      context.write(new StringStringIntWritable(twogram,w1 + " *", decade), count);
      context.write(new StringStringIntWritable(twogram,"* " + w2, decade), count);
    }
  }


  public static class ReducerClass extends Reducer <StringStringIntWritable, IntWritable, Text, SecondPairSumDecadeWritable> {
    private Text two_gram = new Text();
    private SecondPairSumDecadeWritable pairSumDecade = new SecondPairSumDecadeWritable(0,0, "");
    private int currentN = 0;
    private int currentDecade = 0;
    @Override
    public void reduce(StringStringIntWritable key, Iterable<IntWritable> values, Context context) throws IOException,  InterruptedException {
      int sum = 0;
      int decade = key.getDecade();
      String secondPair = key.getSecondPair();
      String twogram = key.getTwogram();

//        < 1960, (shlomo, dani) > {3, 5, 8, 1}
//        < (shlomo, *), 1960 > {3, 5, 8, 1}
//        < (*, dani), 1960 > {3, 5, 8, 1}
        // convert to utf-8

      for (IntWritable count : values) {
        sum += count.get();
      }
//      if (!twogram.contains("*"))
//        currentN += sum;
//      if (currentDecade != decade){
//            context.write(new Text("*N*"), new TwogramSumDecadeWritable(currentN,currentDecade,"") );
//            currentN = 0;
//            currentDecade = decade;
//      }

      two_gram.set(twogram);
      pairSumDecade.setSum(sum);
      pairSumDecade.setDecade(decade);
      pairSumDecade.setSecondPair(secondPair);
      context.write(two_gram, pairSumDecade );
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
    job.setMapOutputKeyClass(StringStringIntWritable.class);
    job.setMapOutputValueClass(IntWritable.class);
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(SecondPairSumDecadeWritable.class);
//    job.setNumReduceTasks(20);
//    job.setInputFormatClass(SequenceFileInputFormat.class);
    FileInputFormat.addInputPath(job, new Path(args[0]));
    FileOutputFormat.setOutputPath(job, new Path(args[1]));
    System.exit(job.waitForCompletion(true) ? 0 : 1);
  }

}

