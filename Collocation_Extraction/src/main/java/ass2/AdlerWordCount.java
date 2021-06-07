package ass2;

import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.giraph.writable.tuple.PairWritable;
//import org.apache.giraph.writable.tuple.PairWritableWritable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class AdlerWordCount {


public static class MapperClass extends Mapper<LongWritable, Text, PairWritable<Text, IntWritable>, IntWritable> {
    private IntWritable decade = new IntWritable(0);
    private IntWritable count = new IntWritable(0);
    private Text twogram = new Text();

    @Override
    public void map(LongWritable key, Text value, Context context) throws IOException,  InterruptedException {
      StringTokenizer itr = new StringTokenizer(value.toString());
      if(itr.countTokens() > 3){
          twogram.set(itr.nextToken() + " " + itr.nextToken());
          String strYear = itr.nextToken();
          decade.set(Integer.parseInt(strYear.substring(0,2) + "0"));
          count.set(Integer.parseInt(itr.nextToken()));
      }
        context.write(new PairWritable(twogram, decade), count);
    }
  }

  public static class ReducerClass extends Reducer<PairWritable<Text,IntWritable>, IntWritable, Text, PairWritable<IntWritable, IntWritable> > {
    @Override
    public void reduce(PairWritable<Text,IntWritable> key, Iterable<IntWritable> values, Context context) throws IOException,  InterruptedException {
      int sum = 0;
      IntWritable decade = key.getValue();
      Text twogram = key.getKey();
      for (IntWritable count : values) {
        sum += count.get();
      }
      context.write(twogram, new PairWritable(new IntWritable(sum), decade) );
    }
  }

    public static class PartitionerClass extends Partitioner<Text, IntWritable> {
      @Override
      public int getPartition(Text key, IntWritable value, int numPartitions) {
        return key.hashCode() % numPartitions;
      }
    }

    static void fix() {
        String OS = System.getProperty("os.name").toLowerCase();

        if (OS.contains("win")) {
            System.setProperty("hadoop.home.dir", "C:\\Users\\yotam\\Desktop\\win_utils");
        } else {
            System.setProperty("hadoop.home.dir", "/");
        }
    }

 public static void main(String[] args) throws Exception {
//    fix();
    Configuration conf = new Configuration();
    Job job = Job.getInstance(conf, "word count");
//    job.setJar("hadoop-mapreduce-client-jobclient-2.2.0.2.0.6.0-76:hadoop-mapreduce-client-shuffle-2.3.0.jar:hadoop-mapreduce-client-common-2.3.0.jar");
    job.setJarByClass(WordCount.class);
    job.setMapperClass(MapperClass.class);
//    job.setPartitionerClass(PartitionerClass.class);
    job.setCombinerClass(ReducerClass.class);
    job.setReducerClass(ReducerClass.class);
    job.setMapOutputKeyClass(Text.class);
    job.setMapOutputValueClass(IntWritable.class);
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(IntWritable.class);
//    job.setNumReduceTasks(20);
    FileInputFormat.addInputPath(job, new Path(args[0]));
    FileOutputFormat.setOutputPath(job, new Path(args[1]));
    System.exit(job.waitForCompletion(true) ? 0 : 1);
  }

}

