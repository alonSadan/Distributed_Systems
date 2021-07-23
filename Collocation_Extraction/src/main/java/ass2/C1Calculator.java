package ass2;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.*;

public class C1Calculator {

    public static class MapperClass extends Mapper<LongWritable, Text, Decade2GramC1C2, StringIntWritable> {
        private Decade2GramC1C2 shlomo = new Decade2GramC1C2(3,"shlomo",0,0,0);
        private StringIntWritable si = new StringIntWritable("si",2);
        private final String stopWordsFileName = "stopWords" + new Date().getTime() + ".txt";
        fileContainsWord stopWordsFinder;
//        @Override
        @Override
        public void setup(Context context) throws IOException {
            //this will be executed once on each mapper before first map(..) call
            S3ObjectOperations.getObject("ass2jar", "stopWords.txt", stopWordsFileName);
            stopWordsFinder = new fileContainsWord(stopWordsFileName);
        }

        public void map(LongWritable key, Text value, Context context) throws IOException,  InterruptedException {
            StringTokenizer itr = new StringTokenizer(value.toString());
            String w1 = null;
            String w2 = null;
            int count = 0;
            int decade = 0;
            if(itr.countTokens() > 3){
                w1 = itr.nextToken();
                w2 = itr.nextToken();
                String strYear = itr.nextToken();
                System.err.println("yotam " + strYear);

                decade = (Integer.parseInt(strYear.substring(0,3) + "0"));
                count = Integer.parseInt(itr.nextToken());
            }
            if (stopWordsFinder.contains(w1) || stopWordsFinder.contains(w2)){
                return;
            }

            si.setStr(w2);
            si.setNumber(count);
            shlomo.setDecade(decade);
            shlomo.setWord(w1);
            shlomo.setFlag(0);
            context.write(shlomo, si);
            shlomo.setFlag(1); // set flag
            context.write(shlomo, si);
        }
    }

    public static class ReducerClass extends Reducer<Decade2GramC1C2, StringIntWritable, Decade2GramC1C2, IntWritable> {
        private IntWritable num = new IntWritable(0);
        private int sum = 0;
//        @Override
        public void reduce(Decade2GramC1C2 key, Iterable<StringIntWritable> values, Context context) throws IOException,  InterruptedException {
            int decade = key.getDecade();
            int flag = key.getFlag();
            String word1 = key.getTwogram();
            if (flag == 1){ // flag
                for (StringIntWritable value : values) {
                    sum += value.getNumber();
                }
            }
            else{
                for (StringIntWritable value : values){
                    num.set(value.getNumber());
                    context.write(new  Decade2GramC1C2(decade,word1 + " " + value.getStr(),
                            sum,0,0), num);
                }
                sum = 0; // initilize sum again
            }
        }
    }

//    public static class PartitionerClass extends Partitioner<Text, IntWritable> {
//      @Override
//      public int getPartition(Text key, IntWritable value, int numPartitions) {
//        return key.hashCode() % numPartitions;
//      }
//    }

   public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
       Job job = Job.getInstance(conf, "c1_calculator");
        job.setJarByClass(C1Calculator.class);
        job.setMapperClass(MapperClass.class);
//    job.setPartitionerClass(PartitionerClass.class);
//    job.setCombinerClass(ReducerClass.class);
        job.setReducerClass(ReducerClass.class);
        job.setMapOutputKeyClass(Decade2GramC1C2.class);
        job.setMapOutputValueClass(StringIntWritable.class);
        job.setOutputKeyClass(Decade2GramC1C2.class);
        job.setOutputValueClass(IntWritable.class);
//    job.setNumReduceTasks(20);
        //job.setInputFormatClass(SequenceFileInputFormat.class);
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1] + new Date().getTime()));
        System.exit(job.waitForCompletion(true) ? 0 : 1);

   }

}



